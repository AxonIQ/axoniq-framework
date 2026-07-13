/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.postgresql;

import io.axoniq.license.entitlement.EntitlementManager;
import io.axoniq.license.entitlement.EntitlementMessageType;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.ContinuousMessageStream;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.Position;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.eventstore.StreamSpliterator;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.DelayedMessageStream;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriterion;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import javax.sql.DataSource;

/**
 * A {@link EventStorageEngine} and {@link SnapshotStore} implementation backed by PostgreSQL, providing
 * reliable event persistence and streaming with support for tagging.
 *
 * @author John Hendrikx
 * @since 5.0.0
 */
public final class PostgresqlEventStorageEngine implements EventStorageEngine, SnapshotStore {

    private record FinalizedEvent(long position, EventMessage event) {}

    /**
     * Represents a batch of events read from the event store.
     * <p>
     * The batch contains the events included in this read, and the highest global index
     * observed in this batch. This value is either:
     * <ul>
     *     <li>the global index of the last event in the batch, if the end of the store was not reached, or</li>
     *     <li>the highest global index scanned in the store, if the end was reached.</li>
     * </ul>
     * An optional snapshot can be contained in the first batch if read with {@link SourcingStrategy.Snapshot}.
     * If the snapshot is present, the events following it are from the position given by the snapshot.
     *
     * @param snapshot the snapshot found, or {@code null} if none was available (or none was compatible
     *                 with the requested maximum position)
     * @param events   the events returned as part of this batch, cannot be {@code null}, but may be empty
     * @param highestGlobalIndex the highest global index observed in this batch
     */
    private record Batch(@Nullable Snapshot snapshot, List<FinalizedEvent> events, long highestGlobalIndex) {}

    private record TagFilter(CharSequence sql, List<List<String>> tagParameters) {
        boolean isEmpty() {
            return tagParameters.isEmpty();
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresqlEventStorageEngine.class);
    private static final TagFilter EMPTY = new TagFilter("", List.of());
    private static final ExecutorService FINALIZER_EXECUTOR = Executors.newSingleThreadExecutor(Thread.ofPlatform().name("PG-Finalizer").factory());  // must be a single thread
    private static final GlobalSequenceTrackingToken GLOBAL_INDEX_START = new GlobalSequenceTrackingToken(1);

    /**
     * Sentinel used as the "no maximum" snapshot position in {@link #load(Set, int, SourcingStrategy)},
     * so that a missing {@link SourcingStrategy.Snapshot#maximumPosition()} can be bound the same way
     * as an actual position, without a separate code path.
     */
    private static final GlobalIndexPosition MAX_GLOBAL_INDEX_POSITION = new GlobalIndexPosition(Long.MAX_VALUE);

    /**
     * CTE prefix resolving {@code snap} to a fixed, given start position, exposed as {@code
     * sort_index} minus one so that {@link #EVENTS_READ_MULTIPLE} can use a uniform {@code >}
     * comparison (see {@link #RESUME_AT_SNAPSHOT} for why). The remaining columns are {@code
     * NULL}, so the leading row {@link #EVENTS_READ_MULTIPLE} always produces from this CTE
     * carries no snapshot data - see {@link #toSnapshot(ResultSet)}.
     * <p>
     * Used for plain (non-snapshot) sourcing and for continuation reads.
     *
     * <li>Parameter 1 {@code long}: the desired start position (inclusive)
     */
    private static final String RESUME_AT_POSITION =
        """
        WITH snap AS (
          SELECT NULL::int8 AS snapshot_position, (?::int8) - 1 AS sort_index,
                 NULL::varchar AS version, NULL::bytea AS payload, NULL::timestamptz AS timestamp, NULL::json AS metadata
        )
        """;

    /**
     * CTE prefix resolving {@code snap} to the latest snapshot compatible with the given maximum
     * position, or to a row with no snapshot data if none is found. Exposes the same column shape
     * as {@link #RESUME_AT_POSITION}, so both can be used interchangeably as a prefix to {@link
     * #EVENTS_READ_MULTIPLE}.
     * <p>
     * {@code sort_index} holds the snapshot's position minus one, so that {@link
     * #EVENTS_READ_MULTIPLE} can use a plain {@code >} comparison without ever tying with the
     * leading {@code snap} row (finalized global indices start at 1, so 0 and -1 can never
     * collide with a real event). That same leading row is therefore always ordered first, since
     * no qualifying event can have a global index below it. The real (unshifted) snapshot
     * position, needed to reconstruct a {@link Snapshot}, is preserved separately as {@code
     * snapshot_position}.
     * <p>
     * Used for the initial page of a {@link SourcingStrategy.Snapshot} sourcing condition.
     *
     * <li>Parameter 1 {@code String}: the qualified name of the entity
     * <li>Parameter 2 {@code String}: the identifier of the entity
     * <li>Parameter 3 {@code long}: the maximum snapshot position to accept (use {@code Long.MAX_VALUE} for no maximum)
     */
    private static final String RESUME_AT_SNAPSHOT =
        """
        WITH snap AS (
          SELECT sn.global_index AS snapshot_position,
                 COALESCE(sn.global_index, 1) - 1 AS sort_index,
                 sn.version, sn.payload, sn.timestamp, sn.metadata
            FROM (VALUES (1)) AS d (x)
            LEFT JOIN (
              SELECT position_value AS global_index, version, payload, timestamp, metadata
                FROM snapshots
                WHERE qualified_name = ? AND identifier = ? AND position_value <= ?
                ORDER BY position_value DESC
                LIMIT 1
            ) sn ON true
        )
        """;

    /**
     * Queries events in order starting from a given global index, limited by the given limit.
     * Must be prefixed with either {@link #RESUME_AT_POSITION} or {@link #RESUME_AT_SNAPSHOT}.
     * <p>
     * Always returns a leading row sourced from {@code snap} - either genuine snapshot data,
     * or a row with no snapshot data if there is none - followed by up to {@code limit} events.
     * See {@link #RESUME_AT_SNAPSHOT} for why this leading row always sorts first. Callers must
     * always skip the first row and bind {@code limit + 1} to account for it.
     *
     * <li>Parameter 1 {@code long}: maximum number of rows to query, excluding the leading {@code snap} row
     */
    private static final String EVENTS_READ_MULTIPLE =
        """
        SELECT s.sort_index AS global_index, s.timestamp, NULL::varchar AS identifier, NULL::varchar AS type, s.payload, s.metadata, s.version, s.snapshot_position
          FROM snap s
        UNION ALL
        SELECT e.global_index, e.timestamp, e.identifier, e.type, e.payload, e.metadata, NULL AS version, NULL::int8 AS snapshot_position
          FROM events e
          WHERE e.global_index > (SELECT sort_index FROM snap)
        ORDER BY global_index
        LIMIT ?;

        SELECT COALESCE(MAX(global_index), 0) AS last_seen
          FROM events;
        """;

    /**
     * The filter sub query to use with {@link #EVENTS_READ_MULTIPLE} when one or more tag
     * filters need to be applied. This is added to the main query as a JOIN on the tags
     * table.
     * <p>
     * The {@code key-value-pairs} place-holder must be replaced with multiple parameter
     * place-holders, one pair for each tag to filter on.
     *
     * <li>Parameter 1 {@code long}: the number of tags that must match (should be equal to number of tags filtered on)
     */
    private static final String FILTER_SUB_QUERY =
        """
        SELECT t.global_index
          FROM tags t
          WHERE t.global_index > (SELECT sort_index FROM snap)
            AND (t.key, t.value) IN ({key-value-pairs})
          GROUP BY t.global_index
          HAVING COUNT(DISTINCT t.key) = ?
        """;

    /**
     * Finds the lowest global index of an event with a timestamp strictly equal or greater
     * than the given timestamp. Returns a single row containing the global index of the
     * first matching event, or the lowest global index that is strictly greater than the
     * current maximum known index, or 1 if there are no events yet.
     *
     * <li>Parameter 1 {@code Instant}: the timestamp to find
     */
    private static final String EVENTS_FIND_TIMESTAMP =
        """
        SELECT COALESCE(
          (SELECT MIN(global_index) FROM events WHERE timestamp >= ?),
          (SELECT MAX(global_index) + 1 FROM events),
          1
        )
        """;

    /**
     * Finds the lowest global index strictly greater than the largest known value. Returns
     * a single row with the result.
     */
    private static final String EVENTS_FIND_NEXT_AVAILABLE_GLOBAL_INDEX =
        """
        SELECT COALESCE(MAX(global_index) + 1, 1) FROM events
        """;

    /**
     * Inserts an event. This generates a new global index which must be used for
     * updating consistency tags, if any.
     *
     * <li>Parameter 1 {@code Instant}: the event timestamp
     * <li>Parameter 2 {@code String}: the event identifier
     * <li>Parameter 3 {@code String}: the event type
     * <li>Parameter 4 {@code byte[]}: the payload as a byte array
     * <li>Parameter 5 {@code String}: the metadata in JSON format
     */
    private static final String EVENTS_INSERT =
        """
        INSERT INTO events (timestamp, identifier, type, payload, metadata)
          VALUES (?, ?, ?, ?, ?::json)
        """;

    /**
     * Inserts a tag associated with an event.
     *
     * <li>Parameter 1 {@code long}: the global index associated with the newly inserted event
     * <li>Parameter 2 {@code String}: the tag key
     * <li>Parameter 3 {@code String}: the tag value
     */
    private static final String TAG_INSERT =
        """
        INSERT INTO tags (global_index, key, value) VALUES (?, ?, ?)
        """;

    /**
     * Upserts a tag with the latest global index, if it would be consistent, after inserting an event.
     *
     * <li>Parameter 1 {@code int}: the tag hash
     * <li>Parameter 2 {@code long}: the global index associated with the newly inserted event
     * <li>Parameter 3 {@code long}: the global index which it must be consistent with
     */
    private static final String CONSISTENCY_TAGS_UPSERT =
        """
        INSERT INTO consistency_tags (tag_hash, global_index) VALUES (?, ?)
          ON CONFLICT (tag_hash) DO UPDATE
            SET global_index = EXCLUDED.global_index
            WHERE consistency_tags.global_index < ? AND consistency_tags.global_index >= 0
        """;

    /**
     * Upserts a tag with the latest global index unconditionally after inserting an event.
     *
     * <li>Parameter 1 {@code int}: the tag hash
     * <li>Parameter 2 {@code long}: the global index associated with the newly inserted event
     */
    private static final String UNCONDITIONAL_CONSISTENCY_TAGS_UPSERT =
        """
        INSERT INTO consistency_tags (tag_hash, global_index) VALUES (?, ?)
          ON CONFLICT (tag_hash) DO UPDATE
            SET global_index = LEAST(consistency_tags.global_index, EXCLUDED.global_index)
        """;

    /*
     * The finalization statement assigns permanent global_index values to any unfinalized events,
     * regardless of which process inserted them, and always returns the current highest global index.
     *
     * It is possible for this statement to run without finding any unfinalized events. This simply means
     * that another process has already finalized them, effectively coalescing multiple finalizations
     * into a single one. If any events were finalized, a notification is sent to listeners via pg_notify.
     *
     * Note: The SELECT computing latest_global_index is written carefully due to PostgreSQL sequence
     * behavior and CTE evaluation order:
     *
     * 1) If no events were finalized, we fall back to the sequence's current last_value to get the latest index.
     * 2) If events were finalized, we use their new_val values because last_value could reflect the
     *    sequence before the CTE updates complete.
     *
     * The pg_notify call signals listeners but does not guarantee they receive it; it only queues the notification.
     */
    private static final String FINALIZE_STATEMENT =
        """
        WITH lock AS (
          SELECT pg_advisory_xact_lock(42)
        ),
        unfinalized_events AS (
          SELECT global_index AS old_val, NEXTVAL('events_monotonic_seq') AS new_val
          FROM events
          WHERE global_index < 0
          ORDER BY global_index DESC
        ),
        finalized_events AS (
          UPDATE events e
          SET global_index = ue.new_val
          FROM unfinalized_events ue
          WHERE e.global_index = ue.old_val
          RETURNING ue.old_val, ue.new_val
        ),
        finalized_tags AS (
          UPDATE tags t
          SET global_index = fe.new_val
          FROM finalized_events fe
          WHERE t.global_index = fe.old_val
          RETURNING fe.new_val
        ),
        finalized_consistency_tags AS (
          UPDATE consistency_tags ct
          SET global_index = fe.new_val
          FROM finalized_events fe
          WHERE ct.global_index = fe.old_val
          RETURNING fe.new_val
        ),
        latest AS (
          SELECT
            COALESCE(MAX(finalized_events.new_val), (SELECT last_value FROM events_monotonic_seq)) AS latest_global_index,
            COUNT(finalized_events.new_val) AS finalized_count
          FROM finalized_events
        )
        SELECT
          latest.latest_global_index,
          CASE WHEN latest.finalized_count > 0
            THEN pg_notify('events_channel', latest.latest_global_index::text)
            ELSE NULL
          END
          FROM latest;
        """;

    private final TransactionalExecutorProvider<Connection> transactionalExecutorProvider;
    private final DataSource dataSource;
    private final EventConverter converter;
    private final EntitlementManager entitlementManager;
    private final PostgresqlSnapshotStore snapshotStore;

    /**
     * This is the maximum number of consistency tags that are kept track of in
     * the consistency tags table to keep this table to a reasonable size. To detect
     * conflicts, only tag hashes are stored, not full tags to prevent infinite growth
     * of the consistency tags table.
     * <p>
     * The size of 1,048,576 entries (2^20) was chosen to accommodate roughly 500
     * concurrent appends, assuming an average of 3 tags per event, while keeping the
     * hash collision rate below 1%.
     * <p>
     * Setting it higher has the trade off of making the consistency tags table larger
     * requiring more table and index space, and may make it harder to keep this table
     * in memory. Setting it lower has the trade off of making collisions more frequent
     * which can lead to appends being aborted and retried when there was no actual
     * conflict.
     * <p>
     * Must be a power of two to allow efficient bitmask-based indexing.
     */
    private final int hashCapacity = 1024 * 1024;  // must be a power of 2

    /**
     * Derived from capacity (which must be a power of 2)
     */
    private final int hashMask = hashCapacity - 1;

    /*
     * This can become configurable at some later stage with a big warning that it can't be
     * changed easily later on; also, we may want to add detection if an existing store is
     * accidentally configured with an engine with different settings (ie. different hash
     * or different size) as that would be very bad.
     */

    private final HashPolicy hashPolicy = new HashPolicy() {
        @Override
        public int hash(byte[] data) {
            return MurmurHash3.hash32(data);
        }
    };

    /**
     * A static {@link AppendTransaction} implementation. As this engine doesn't manage its own
     * transactions, but leaves this up to hooks installed in the processing lifecycle, this
     * class only needs to trigger finalization on succesful commits.
     */
    private final AppendTransaction<Object> appendTransaction = new AppendTransaction<>() {
        @Override
        public CompletableFuture<Object> commit() {
            return CompletableFuture.completedFuture(null);  // Do nothing during the COMMIT phase
        }

        @Override
        public void rollback() {
            // Do nothing, transactions are not managed by this class
        }

        @Override
        public CompletableFuture<ConsistencyMarker> afterCommit(Object commitResult) {
            return scheduleFinalization();
        }
    };

    /**
     * Watches for new events arriving via Postgres {@code LISTEN}/{@code NOTIFY} and notifies
     * registered stream callbacks.
     */
    private final PostgresqlEventMonitor eventMonitor;

    /**
     * Synchronized field. Future for the queued finalization which append transactions
     * can return in the {@link AppendTransaction#afterCommit(Object, ProcessingContext)}.
     */
    private CompletableFuture<ConsistencyMarker> queuedFinalization;

    /**
     * Synchronized field. Tracks whether any finalizer is running currently.
     */
    private boolean finalizerRunning;

    /**
     * Constructs a new instance.
     *
     * @param dataSource a data source to connect to PostgreSQL, cannot be {@code null}
     * @param converter  an event converter for converting the payload to bytes, cannot be {@code null}
     */
    public PostgresqlEventStorageEngine(DataSource dataSource, EventConverter converter) {
        this(dataSource, converter, EntitlementManager.INSTANCE);
        EntitlementManager.INSTANCE.registerAddon(PostgresAxoniqAddon.class);
    }

    /**
     * Package-private constructor for testing, allowing injection of an alternative {@link EntitlementManager}.
     * Production code must use {@link #PostgresqlEventStorageEngine(DataSource, EventConverter)}, which
     * uses {@link EntitlementManager#INSTANCE} directly.
     *
     * @param dataSource         a data source to connect to PostgreSQL, cannot be {@code null}
     * @param converter          an event converter for converting the payload to bytes, cannot be {@code null}
     * @param entitlementManager the entitlement manager to use, cannot be {@code null}
     */
    @Internal
    PostgresqlEventStorageEngine(DataSource dataSource, EventConverter converter, EntitlementManager entitlementManager) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.converter = Objects.requireNonNull(converter, "converter");
        this.entitlementManager = Objects.requireNonNull(entitlementManager, "entitlementManager");
        this.transactionalExecutorProvider = new JdbcTransactionalExecutorProvider(dataSource);
        this.snapshotStore = new PostgresqlSnapshotStore(dataSource, converter);

        // TODO #7 Allow to configure tables, sequences and indices
        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
        ) {
            statement.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                  global_index INT8 NOT NULL GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY -1),

                  timestamp TIMESTAMPTZ NOT NULL,
                  payload BYTEA,
                  metadata JSON NOT NULL,
                  identifier VARCHAR NOT NULL,
                  type VARCHAR NOT NULL,

                  -- keys
                  PRIMARY KEY (global_index)
                );

                CREATE TABLE IF NOT EXISTS tags (
                  global_index INT8 NOT NULL,

                  key VARCHAR NOT NULL,
                  value VARCHAR NOT NULL,

                  -- keys
                  PRIMARY KEY (key, value, global_index)
                );

                CREATE TABLE IF NOT EXISTS consistency_tags (
                  tag_hash INT4 NOT NULL,
                  global_index INT8 NOT NULL,

                  -- keys
                  PRIMARY KEY (tag_hash)
                );

                -- Create a sequence used for monotonic final global index values. Starts at 1.
                CREATE SEQUENCE IF NOT EXISTS events_monotonic_seq
                  INCREMENT BY 1
                  CACHE 1
                  OWNED BY events.global_index;

                -- BTREE index on global_index in consistency_tags (for faster finalizations)
                CREATE INDEX IF NOT EXISTS consistency_tags_global_index_idx
                  ON consistency_tags (global_index);

                -- BTREE index on global_index in tags (for faster finalizations)
                CREATE INDEX IF NOT EXISTS tags_global_index_idx
                  ON tags (global_index);
                """
            );

            connection.commit();
        }
        catch (SQLException e) {
            throw new IllegalStateException("Could not initialize " + getClass().getSimpleName(), e);
        }

        this.eventMonitor = new PostgresqlEventMonitor(dataSource);
    }

    void close() {  // for testing purposes, to avoid junk exceptions
        eventMonitor.close();
    }

    /*
     * These two methods are the only ones backed by snapshotStore (PostgresqlSnapshotStore).
     * Sourcing with SourcingStrategy.Snapshot instead reads the snapshots table directly via
     * RESUME_AT_SNAPSHOT, as part of the same query as the tail events - see load(Set, int,
     * SourcingStrategy). That duplication is intentional: it is what makes the single-round-trip
     * source-with-snapshot query possible, whereas going through snapshotStore.load() here would
     * mean a separate round trip before the events could be read.
     */

    @Override
    public CompletableFuture<Void> store(QualifiedName qualifiedName, Object identifier, Snapshot snapshot) {
        return snapshotStore.store(qualifiedName, identifier, snapshot);
    }

    @Override
    public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier) {
        return snapshotStore.load(qualifiedName, identifier);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("dataSource", dataSource);
        descriptor.describeProperty("converter", converter);
    }

    @Override
    public CompletableFuture<AppendTransaction<?>> appendEvents(
        AppendCondition condition,
        ProcessingContext context,
        List<TaggedEventMessage<?>> events
    ) {
        entitlementManager.claimMessage(PostgresAxoniqAddon.IDENTIFIER, EntitlementMessageType.EVENT, events.size());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("appendEvents: called with condition=" + condition + ", events=" + events + ", context=" + context);
        }

        return connectionExecutor(context).apply(connection -> {
            if (!internalAppendEvents(connection, condition, events)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("appendEvents: failed");
                }

                throw AppendEventsTransactionRejectedException.conflictingEventsDetected(condition.consistencyMarker());  // allow executor to rollback correctly
            }

            return appendTransaction;
        });
    }

    // TODO #8 performance improvement possible here by avoiding a lot of back-and-forth with the server
    private boolean internalAppendEvents(Connection connection, AppendCondition condition, List<TaggedEventMessage<?>> events) throws SQLException {
        try (
            PreparedStatement eventInsert = connection.prepareStatement(EVENTS_INSERT, Statement.RETURN_GENERATED_KEYS);
            PreparedStatement consistencyTagsUpsert = connection.prepareStatement(CONSISTENCY_TAGS_UPSERT);
            PreparedStatement unconditionalConsistencyTagsUpsert = connection.prepareStatement(UNCONDITIONAL_CONSISTENCY_TAGS_UPSERT);
            PreparedStatement tagInsert = connection.prepareStatement(TAG_INSERT);
        ) {
            Set<Tag> batchLockedTags = new HashSet<>();

            for (TaggedEventMessage<?> tem : events) {
                EventMessage message = tem.event();

                eventInsert.setTimestamp(1, Timestamp.from(message.timestamp()));
                eventInsert.setString(2, message.identifier());
                eventInsert.setString(3, message.type().toString());
                eventInsert.setBytes(4, converter.convertPayload(message, byte[].class));
                eventInsert.setString(5, MetadataSerializer.toJson(message.metadata()));
                eventInsert.execute();

                try (ResultSet keys = eventInsert.getGeneratedKeys()) {
                    if (!keys.next()) {
                        throw new IllegalStateException("Generated keys were expected. Please upgrade your JDBC driver.");
                    }

                    long temporaryGlobalIndex = keys.getLong(1);
                    Set<Tag> lockedTags = lock(consistencyTagsUpsert, condition, temporaryGlobalIndex, batchLockedTags);

                    if (lockedTags == null) {  // locking of some or all tags failed, return that appending failed
                        return false;
                    }

                    batchLockedTags.addAll(lockedTags);

                    // Update unconditional tags (tags not part of the append condition):
                    for (Tag tag : tem.tags()) {
                        if (!lockedTags.contains(tag)) {
                            int hash = hashPolicy.hash((tag.key() + ":" + tag.value()).getBytes(StandardCharsets.UTF_8));

                            unconditionalConsistencyTagsUpsert.setInt(1, hash & hashMask);
                            unconditionalConsistencyTagsUpsert.setLong(2, temporaryGlobalIndex);
                            unconditionalConsistencyTagsUpsert.execute();
                        }

                        tagInsert.setLong(1, temporaryGlobalIndex);
                        tagInsert.setString(2, tag.key());
                        tagInsert.setString(3, tag.value());
                        tagInsert.execute();
                    }
                }
            }

            return true;  // appending was successful
        }
    }

    @Override
    public MessageStream<EventMessage> source(SourcingCondition condition) {
        Set<EventCriterion> criterions = condition.criteria().flatten();

        return DelayedMessageStream.create(
            load(criterions, 50, condition.strategy())
                .thenApply(initial -> buildStream(initial, criterions))
        );
    }

    /**
     * Builds the stream for the initial page of a sourcing condition: a leading
     * {@link SnapshotEventMessage} if {@code initial} carries a snapshot (only possible for
     * {@link SourcingStrategy.Snapshot} - {@link SourcingStrategy.Absolute} never produces one),
     * followed by the events already fetched with it, followed by any further events fetched via
     * the regular pagination path.
     *
     * @param initial the initial batch, cannot be {@code null}
     * @param criterions the tag criteria used to fetch further pages, cannot be {@code null}
     * @return the resulting stream, never {@code null}
     */
    private MessageStream<EventMessage> buildStream(Batch initial, Set<EventCriterion> criterions) {
        AtomicLong lastGlobalIndex = new AtomicLong(initial.highestGlobalIndex);

        MessageStream<EventMessage> tail = MessageStream.fromStream(
            initial.events.stream(),
            FinalizedEvent::event,
            PostgresqlEventStorageEngine::trackingTokenContext
        ).concatWith(internalStream(criterions, initial.highestGlobalIndex + 1, lastGlobalIndex, List::isEmpty));

        MessageStream<EventMessage> withSnapshot = initial.snapshot == null
            ? tail
            : MessageStream.<EventMessage>just(new SnapshotEventMessage(initial.snapshot)).concatWith(tail);

        return withConsistencyMarkerTerminal(withSnapshot, lastGlobalIndex);
    }

    /**
     * Appends the terminal {@link TerminalEventMessage}, carrying the consistency marker for the
     * position just after {@code lastGlobalIndex}, once {@code body} completes.
     * <p>
     * Relies on {@link MessageStream#concatWith(Supplier)}'s lazy supplier, which is only invoked
     * once a consumer has fully drained {@code body} - so {@code lastGlobalIndex} is guaranteed to
     * hold its final value by the time it is read here, without needing a {@link CompletableFuture}
     * to explicitly wait for completion first.
     *
     * @param body the stream to append the terminal message to, cannot be {@code null}
     * @param lastGlobalIndex the highest global index observed while producing {@code body},
     *                        updated by its own completion before this method is called
     * @return {@code body}, followed by the terminal message, never {@code null}
     */
    private static MessageStream<EventMessage> withConsistencyMarkerTerminal(MessageStream<EventMessage> body, AtomicLong lastGlobalIndex) {
        return body.concatWith(() -> MessageStream.just(
            TerminalEventMessage.INSTANCE,
            unused -> Context.with(
                ConsistencyMarker.RESOURCE_KEY,
                new GlobalIndexConsistencyMarker(lastGlobalIndex.get() + 1)  // return index of potential next matching message
            )
        ));
    }

    @Override
    public MessageStream<EventMessage> stream(StreamingCondition condition) {
        Set<EventCriterion> criterions = condition.criteria().flatten();
        TrackingToken trackingToken = condition.position();

        if (trackingToken != null && !(trackingToken instanceof GlobalSequenceTrackingToken)) {
            throw new IllegalArgumentException(
                "Tracking Token is not of expected type. Must be GlobalSequenceTrackingToken. Is: "
                    + trackingToken.getClass().getName()
            );
        }

        AtomicLong nextQueryIndex = new AtomicLong(trackingToken == null ? 0 : Math.max(0, trackingToken.position().orElse(0)));

        Supplier<List<FinalizedEvent>> fetcher = () -> {
            long position = nextQueryIndex.get();
            Batch batch = load(criterions, 50, new SourcingStrategy.Absolute(new GlobalIndexPosition(position))).join();

            /*
             * The above code joins on the future, but ContinuousMessageStream should probably
             * accept a future here for the fetcher; it should then probably also only do the
             * has next available callback when the events have been fetched since #peek and #next
             * on MessageStream don't return completable futures. In other words, ContinuousMessageStream
             * needs a bit of an adjustment to keep this fully async.
             */

            nextQueryIndex.set(batch.highestGlobalIndex + 1);

            return batch.events;
        };

        return new ContinuousMessageStream<>(
            fetcher,
            this::toMessageStreamEntry,
            eventMonitor::registerCallback
        );
    }

    private SimpleEntry<EventMessage> toMessageStreamEntry(FinalizedEvent finalizedEvent) {
        return new SimpleEntry<>(finalizedEvent.event, trackingTokenContext(finalizedEvent));
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken() {
        return CompletableFuture.completedFuture(GLOBAL_INDEX_START);
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken() {
        return connectionExecutor(null).apply(connection -> {
            try (
                PreparedStatement ps = connection.prepareStatement(EVENTS_FIND_NEXT_AVAILABLE_GLOBAL_INDEX);
                ResultSet resultSet = ps.executeQuery();
            ) {
                resultSet.next();

                long globalIndex = resultSet.getLong(1);

                return new GlobalSequenceTrackingToken(globalIndex);
            }
        });
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at) {
        return connectionExecutor(null).apply(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(EVENTS_FIND_TIMESTAMP)) {

                /*
                 * Note: timestamps can't be guaranteed to be in the exact same order
                 * as the global index, so it is possible some older timestamps are
                 * encountered when starting at a specific timestamp.
                 */

                statement.setTimestamp(1, Timestamp.from(at));

                try (ResultSet resultSet = statement.executeQuery()) {
                    resultSet.next();

                    return new GlobalSequenceTrackingToken(resultSet.getLong(1));
                }
            }
        });
    }

    private MessageStream<EventMessage> internalStream(
        Set<EventCriterion> criterions,
        long start,
        AtomicLong lastGlobalIndex,
        Predicate<List<? extends FinalizedEvent>> predicate
    ) {
        StreamSpliterator<FinalizedEvent> entrySpliterator = new StreamSpliterator<>(
            last -> {
                long position = last == null ? start : last.position + 1;
                Batch batch = load(criterions, 50, new SourcingStrategy.Absolute(new GlobalIndexPosition(position))).join();

                lastGlobalIndex.set(batch.highestGlobalIndex);

                return batch.events;
            },
            predicate
        );

        return MessageStream.fromStream(
            StreamSupport.stream(entrySpliterator, false),
            FinalizedEvent::event,
            PostgresqlEventStorageEngine::trackingTokenContext
        );
    }

    /**
     * Loads a batch of events according to the given {@code sourcingStrategy}, in a single round
     * trip. For {@link SourcingStrategy.Snapshot}, the latest compatible snapshot (if any) is
     * looked up and returned as part of the same query, rather than as a separate round trip -
     * see {@link #RESUME_AT_SNAPSHOT}. For {@link SourcingStrategy.Absolute}, the batch's
     * {@code snapshot} is always {@code null}.
     *
     * @param criterions the tag criteria to filter events on, cannot be {@code null}, but may be empty
     * @param limit the maximum number of events to return
     * @param sourcingStrategy the strategy determining the start position, or the snapshot to look
     *                         up, cannot be {@code null}
     * @return a future with the batch, never {@code null}
     */
    // TODO #9 Prefetching via max parameter here should perhaps not be a concern of the engine
    private CompletableFuture<Batch> load(Set<EventCriterion> criterions, int limit, SourcingStrategy sourcingStrategy) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("load: loading from " + sourcingStrategy + " (limit " + limit + ") with condition " + criterions);
        }

        TagFilter tagFilter = buildTagFilter(criterions);
        String queryPrefix = switch (sourcingStrategy) {
            case SourcingStrategy.Absolute a -> RESUME_AT_POSITION;
            case SourcingStrategy.Snapshot s -> RESUME_AT_SNAPSHOT;
        };
        String queryFilter = tagFilter.isEmpty()
            ? EVENTS_READ_MULTIPLE
            : EVENTS_READ_MULTIPLE.replace("FROM events e", "FROM events e JOIN (" + tagFilter.sql + ") USING (global_index)");
        String query = queryPrefix + queryFilter;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("load: using query: " + query + " from filter: " + tagFilter);
        }

        long positionValue = switch (sourcingStrategy) {
            case SourcingStrategy.Absolute(Position p) -> Math.max(0, GlobalIndexPosition.toIndex(p));
            case SourcingStrategy.Snapshot s -> GlobalIndexPosition.toIndex(s.maximumPosition() == null ? MAX_GLOBAL_INDEX_POSITION : s.maximumPosition());
        };

        return connectionExecutor(null).apply(connection -> {
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                int parameterIndex = 1;

                if (sourcingStrategy instanceof SourcingStrategy.Snapshot s) {
                    ps.setString(parameterIndex++, s.qualifiedName().fullName());
                    ps.setString(parameterIndex++, String.valueOf(s.identifier()));
                }

                ps.setLong(parameterIndex++, positionValue);  // RESUME_AT_POSITION's start position or RESUME_AT_SNAPSHOT's maximum position

                for (List<String> parameterGroup : tagFilter.tagParameters) {
                    for (String parameter : parameterGroup) {
                        ps.setString(parameterIndex++, parameter);  // tag parameters in subquery
                    }

                    ps.setInt(parameterIndex++, parameterGroup.size() / 2);  // HAVING COUNT in each subquery
                }

                ps.setLong(parameterIndex++, limit + 1L);  // LIMIT ?, +1 for the leading snap row

                if (!ps.execute()) {
                    throw new IllegalStateException("A ResultSet is expected");
                }

                Snapshot snapshot;
                List<FinalizedEvent> list = new ArrayList<>();

                try (ResultSet resultSet = ps.getResultSet()) {
                    resultSet.next();  // always present, holds no event data in the plain (non-snapshot) case

                    snapshot = toSnapshot(resultSet);

                    while (resultSet.next()) {
                        list.add(toFinalizedEvent(resultSet));
                    }
                }

                if (list.size() == limit) {

                    /*
                     * If limit was reached, the maximum global index query is not yet needed
                     * as there will be more queries needed to finish the stream. Don't
                     * bother fetching it and just return early:
                     */

                    return new Batch(snapshot, list, list.getLast().position);
                }

                if (!ps.getMoreResults()) {
                    throw new IllegalStateException("A second ResultSet is expected");
                }

                try (ResultSet resultSet = ps.getResultSet()) {
                    resultSet.next();

                    long maxGlobalIndex = resultSet.getLong(1);

                    return new Batch(snapshot, list, maxGlobalIndex);
                }
            }
        });
    }

    private FinalizedEvent toFinalizedEvent(ResultSet resultSet) throws SQLException {
        long globalIndex = resultSet.getLong(1);
        Instant timestamp = resultSet.getTimestamp(2).toInstant();
        String identifier = resultSet.getString(3);
        MessageType messageType = MessageType.fromString(resultSet.getString(4));
        byte[] payload = resultSet.getBytes(5);
        Map<String, String> metadata = MetadataSerializer.fromJson(resultSet.getString(6));

        return new FinalizedEvent(
            globalIndex,
            new GenericEventMessage(identifier, messageType, payload, metadata, timestamp)
                .withConverter(converter)
        );
    }

    /**
     * Reconstructs the {@link Snapshot} from the leading row produced by {@link #EVENTS_READ_MULTIPLE}
     * when prefixed with {@link #RESUME_AT_SNAPSHOT}, or returns {@code null} if that row carries no
     * snapshot data (a {@code NULL} payload).
     *
     * @param resultSet the result set, positioned at the leading {@code snap} row, cannot be {@code null}
     * @return the snapshot, or {@code null} if none was found
     * @throws SQLException when a JDBC error occurred
     */
    private static @Nullable Snapshot toSnapshot(ResultSet resultSet) throws SQLException {
        byte[] payload = resultSet.getBytes(5);

        if (payload == null) {
            return null;
        }

        long position = resultSet.getLong(8);
        String version = resultSet.getString(7);
        Instant timestamp = resultSet.getTimestamp(2).toInstant();
        Map<String, String> metadata = MetadataSerializer.fromJson(resultSet.getString(6));

        return new Snapshot(new GlobalIndexPosition(position), version, payload, timestamp, metadata);
    }

    private static Context trackingTokenContext(FinalizedEvent event) {
        return TrackingToken.addToContext(Context.empty(), new GlobalSequenceTrackingToken(event.position + 1));
    }

    private static TagFilter buildTagFilter(Set<EventCriterion> criterions) {
        if (criterions.isEmpty()) {
            return EMPTY;
        }

        StringBuilder sql = new StringBuilder();
        List<List<String>> parameterGroups = new ArrayList<>();
        boolean firstCriterion = true;

        for (EventCriterion criterion : criterions) {
            if (criterion.tags().isEmpty()) {
                return EMPTY;  // no tag restriction means match all events, so no filter needed
            }

            if (!firstCriterion) {
                sql.append(" UNION ");
            }

            List<String> parameters = new ArrayList<>();
            StringBuilder keyValuePairs = new StringBuilder();

            for (Tag tag : criterion.tags()) {
                if (!keyValuePairs.isEmpty()) {
                    keyValuePairs.append(", ");
                }

                keyValuePairs.append("(?, ?)");

                parameters.add(tag.key());
                parameters.add(tag.value());
            }

            parameterGroups.add(parameters);

            sql.append(FILTER_SUB_QUERY.replace("{key-value-pairs}", keyValuePairs));

            firstCriterion = false;
        }

        return new TagFilter(sql, parameterGroups);
    }

    /**
     * This function "locks" the tags part of the given append condition, so other concurrent
     * events being appended using overlapping tags will block. If this transaction is committed,
     * any other transactions blocking on an overlapping tag will fail. If this transaction is
     * rolled back, they may progress.
     *
     * The locking works by modifying the consistency tags table, and setting the global index
     * for each of the tags involved to the global index value of the event that was just inserted.
     * This is a temporary value (negative), which will be updated to a permanent global index
     * as part of a separate finalization transaction.
     *
     * Note that encountering a global index during the update that is either higher than the index
     * given in the append condition, or is a temporary (negative) index, means there was a conflict.
     *
     * @param tagUpsert the conditional tag upsert statement, cannot be {@code null}
     * @param condition the append condition, cannot be {@code null}
     * @param temporaryGlobalIndex the (temporary) index of a newly inserted event, always negative
     * @param batchLockedTags tags already locked in this batch, cannot be {@code null}
     * @return a set of tags that were locked (possibly empty), or {@code null} if locking failed
     * @throws SQLException when a JDBC error occurred
     */
    private Set<Tag> lock(PreparedStatement tagUpsert, AppendCondition condition, long temporaryGlobalIndex, Set<Tag> batchLockedTags) throws SQLException {

        /*
         * The position in a consistency marker is the position just after the last event it was consistent with,
         * as this position is the position one can resume a sourcing from without having to adjust it manually.
         * The locking insert/update query used uses a less than comparison ("<"), so this will work correctly, without
         * having to adjust the marker position.
         */

        long globalIndex = Math.max(0, GlobalIndexConsistencyMarker.position(condition.consistencyMarker()));

        assert temporaryGlobalIndex < 0;
        assert globalIndex >= 0;

        Set<Tag> lockedTags = new HashSet<>();

        for(EventCriterion criterion : condition.criteria().flatten()) {
            // TODO #52 Support type based append transactions
            for (Tag tag : criterion.tags()) {
                // Only lock the tag if it wasn't already locked in this batch:
                if (!batchLockedTags.contains(tag)) {
                    int hash = hashPolicy.hash((tag.key() + ":" + tag.value()).getBytes(StandardCharsets.UTF_8));

                    tagUpsert.setInt(1, hash & hashMask);
                    tagUpsert.setLong(2, temporaryGlobalIndex);  // the temporary global index to write
                    tagUpsert.setLong(3, globalIndex);  // the (permanent) global index to check for consistency (never negative)

                    if (tagUpsert.executeUpdate() == 0) {
                        return null;
                    }
                }

                lockedTags.add(tag);
            }
        }

        return lockedTags;
    }

    private synchronized CompletableFuture<ConsistencyMarker> scheduleFinalization() {
        if (!finalizerRunning) {
            finalizerRunning = true;

            return CompletableFuture.supplyAsync(this::runFinalizationTask, FINALIZER_EXECUTOR);
        }

        if (queuedFinalization == null) {
            queuedFinalization = CompletableFuture.supplyAsync(this::runFinalizationTask, FINALIZER_EXECUTOR);
        }

        return queuedFinalization;
    }

    private ConsistencyMarker runFinalizationTask() {
        try {
            long latestGlobalIndex = finalizeAndReturnLatestIndex();

            eventMonitor.updateHighestKnownGlobalIndex(latestGlobalIndex);  // will notify callbacks if needed

            return new GlobalIndexConsistencyMarker(latestGlobalIndex + 1);
        }
        catch (SQLException e) {

            /*
             * Throwing an exception here means that the AppendCondition#afterCommit
             * method will return a failed future, and no consistency marker. The
             * framework should deal with this. Note that the events are still
             * already committed and permanent, we're just unable to determine the
             * correct marker.
             */

            throw new IllegalStateException("Finalization failed", e);
        }
        finally {
            synchronized (this) {
                if (queuedFinalization == null) {
                    finalizerRunning = false;
                }
                else {
                    queuedFinalization = null;
                }
            }
        }
    }

    private long finalizeAndReturnLatestIndex() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {

            /*
             * Finalization is completely independent of any other transactions, and
             * the connection therefore can be modified to suit finalization needs.
             *
             * As finalization is modifying the consistency tags table, an isolation
             * level higher than TRANSACTION_READ_COMMITTED would result in many
             * serialization retries, potentially even completely blocking the finalizer
             * in a busy event store. As the finalizer only needs a single consistent
             * view of the temporary indices in the events table to proceed, there is
             * no need to guarantee this view has remained unchanged over the course
             * of the transaction.
             */

            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            /*
             * The finalization statement takes all temporary IDs, assigns them permanent
             * IDs, and then returns the latest permanent value assigned from the monotonic
             * sequence. This value either reflects the global index of the last event it
             * finalized, or if there was nothing to finalize, it simply reflects the global
             * index of the last event finalized by any finalize run.
             *
             * Empty finalization runs can occur for two reasons:
             *
             * - Another JVM did the finalization.
             *
             * - A finalization was triggered by one transaction, and another concurrently
             *   shortly after it. The second finalization is queued to be absolutely sure
             *   it will include the events of the second transaction. However, if the events
             *   were committed and visible before the first finalizer started its work,
             *   it may include them already. The second finalization then may see no events
             *   to finalize, but simply returns the latest global index.
             *
             * Note: A finalization run may include events that were committed after the
             * events that triggered it and which do not actually share the same consistency
             * tags. As a result, the returned global index may be slightly higher than
             * strictly required for consistency. This is expected and safe: it still
             * provides a valid high-water mark after which new events with potentially
             * conflicting tags can be appended.
             */

            try (
                PreparedStatement ps = connection.prepareStatement(FINALIZE_STATEMENT);
                ResultSet resultSet = ps.executeQuery();
            ) {
                resultSet.next();  // query always returns a single row

                long globalIndex = resultSet.getLong(1);

                connection.commit();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("finalizePositions completed with latest global index: " + globalIndex);
                }

                return globalIndex;
            }
        }
    }

    private TransactionalExecutor<Connection> connectionExecutor(ProcessingContext processingContext) {
        return transactionalExecutorProvider.getTransactionalExecutor(processingContext);
    }

    /**
     * Defines a pluggable hash policy.
     */
    interface HashPolicy {

        /**
         * Computes a hash of the given data.
         *
         * @param data the input byte array to hash, cannot be {@code null} but can be empty
         * @return a 32-bit hash code for the input
         */
        int hash(byte[] data);

    }
}
