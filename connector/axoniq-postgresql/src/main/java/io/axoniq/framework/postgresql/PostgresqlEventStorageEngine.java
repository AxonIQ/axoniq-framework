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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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

    /**
     * The parameters needed to bind one criterion's {@link #FILTER_SUB_QUERY} instance, in the
     * order its {@code ?} place-holders appear: the criterion's tag key/value pairs, then its
     * type array (if it restricts by type), then the distinct key/value pair count for {@code HAVING}.
     *
     * @param tagParameters    the criterion's tag keys and values, alternating, cannot be {@code null}, may be empty
     * @param typeParameters   the criterion's type names, cannot be {@code null}, empty if the criterion does not restrict by type
     * @param distinctTagCount the number of distinct tags required to match: one per tag, plus one more if typeParameters is non-empty
     */
    private record CriterionFilter(List<String> tagParameters, List<String> typeParameters, int distinctTagCount) {}

    private record TagFilter(CharSequence sql, List<CriterionFilter> criterionFilters) {
        boolean isEmpty() {
            return criterionFilters.isEmpty();
        }
    }

    /**
     * A single criterion's {@code condition} fragment (for {@link #FILTER_SUB_QUERY} or
     * {@link #CONSISTENCY_TAGS_LOCK}'s fallback) paired with the {@link CriterionFilter} bind
     * parameters that go with it - see {@link #buildCriterionSql(EventCriterion)}.
     *
     * @param condition the criterion's tag/type condition, cannot be {@code null}
     * @param filter    the criterion's bind parameters, cannot be {@code null}
     */
    private record CriterionSql(String condition, CriterionFilter filter) {}

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresqlEventStorageEngine.class);
    private static final TagFilter EMPTY = new TagFilter("", List.of());
    private static final GlobalSequenceTrackingToken GLOBAL_INDEX_START = new GlobalSequenceTrackingToken(1);

    /**
     * Sentinel used as the "no maximum" snapshot position in {@link #load(Set, int, SourcingStrategy)},
     * so that a missing {@link SourcingStrategy.Snapshot#maximumPosition()} can be bound the same way
     * as an actual position, without a separate code path.
     */
    private static final GlobalIndexPosition MAX_GLOBAL_INDEX_POSITION = new GlobalIndexPosition(Long.MAX_VALUE);

    /**
     * Reserved tag key used to record an event's type as a regular tag, written automatically by
     * the {@code axon_write_type_tag} database trigger installed in the constructor. Callers
     * cannot supply a tag using this key themselves; see {@link #validateNoReservedTags(List)}.
     * <p>
     * Package-private so tests can construct the exact same {@code consistency_tags} identity
     * this engine uses for the reserved type restriction.
     */
    static final String TYPE_TAG_KEY = "__T";

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
     * <p>
     * The trailing {@code type_version} column is the event's {@code MessageType} version - not to
     * be confused with the {@code version} column, which is the leading snapshot row's own version.
     *
     * <li>Parameter 1 {@code long}: maximum number of rows to query, excluding the leading {@code snap} row
     */
    private static final String EVENTS_READ_MULTIPLE =
        """
        SELECT s.sort_index AS global_index, s.timestamp, NULL::varchar AS identifier, NULL::varchar AS type, s.payload, s.metadata, s.version, s.snapshot_position, NULL::varchar AS type_version
          FROM snap s
        UNION ALL
        SELECT e.global_index, e.timestamp, e.identifier, e.type, e.payload, e.metadata, NULL AS version, NULL::int8 AS snapshot_position, e.type_version
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
     * The {@code condition} place-holder must be replaced with the criterion's tag and/or type
     * condition - see {@link #buildTagFilter(Set)}.
     *
     * <li>Parameter 1 {@code long}: the number of distinct key/value pairs that must match (one per
     * tag, plus one more for the reserved type tag if the criterion also restricts by type)
     */
    private static final String FILTER_SUB_QUERY =
        """
        SELECT t.global_index
          FROM tags t
          WHERE t.global_index > (SELECT sort_index FROM snap)
            AND ({condition})
          GROUP BY t.global_index
          HAVING COUNT(DISTINCT (t.key, t.value)) = ?
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
     * <li>Parameter 3 {@code String}: the event type's qualified name
     * <li>Parameter 4 {@code String}: the event type's version
     * <li>Parameter 5 {@code byte[]}: the payload as a byte array
     * <li>Parameter 6 {@code String}: the metadata in JSON format
     */
    private static final String EVENTS_INSERT =
        """
        INSERT INTO events (timestamp, identifier, type, type_version, payload, metadata)
          VALUES (?, ?, ?, ?, ?, ?::json)
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
     * Locks one or more attribute hashes for a single criterion using only the fast, same-row
     * check - no fallback. This is the first of up to two round trips - see {@link #lockCriterion}
     * for why the fallback-aware statement ({@link #CONSISTENCY_TAGS_LOCK}) must always be a
     * separate, later round trip rather than be embedded here.
     * <p>
     * The {@code {values}} place-holder must be replaced with one {@code (?, ?)} group per
     * attribute hash to lock.
     *
     * <li>Parameter group, repeated once per attribute hash: {@code int} the tag hash, {@code long} the temporary global index
     * <li>Final parameter {@code long}: the marker position
     */
    private static final String CONSISTENCY_TAGS_FAST_LOCK =
        """
        INSERT INTO consistency_tags (tag_hash, global_index) VALUES {values}
          ON CONFLICT (tag_hash) DO UPDATE
            SET global_index = LEAST(consistency_tags.global_index, EXCLUDED.global_index)
            WHERE consistency_tags.global_index < ? AND consistency_tags.global_index >= 0
          RETURNING tag_hash
        """;

    /**
     * Locks one or more still-unresolved attribute hashes for a single criterion, upserting
     * each with the latest (temporary) global index if it would be consistent - either because
     * nothing has touched it since the given marker (the fast check, re-tried here in case it
     * was only contention, not a real conflict, that made {@link #CONSISTENCY_TAGS_FAST_LOCK}
     * leave it unresolved), or, failing that, because a precise re-check against the real
     * {@code tags}/{@code events} data confirms no event actually matching the full criterion
     * exists beyond the marker (the fallback).
     * <p>
     * The {@code {values}} place-holder must be replaced with one {@code (?, ?)} group per
     * attribute hash to lock. The {@code {condition}} place-holder must be replaced with the
     * criterion's tag/type condition - see {@link #buildCriterionSql(EventCriterion)}.
     * <p>
     * This must always run as its own, separate round trip from {@link #CONSISTENCY_TAGS_FAST_LOCK}
     * - never embedded in the same statement - so that its fallback subquery is guaranteed a
     * fresh snapshot. Verified empirically against PostgreSQL 16: a statement that has to wait
     * for a row lock held by another transaction only gets a refreshed view of that specific row
     * once unblocked (the standard {@code READ COMMITTED} re-check) - a subquery against a
     * different table, evaluated as part of that same blocked statement, still sees the snapshot
     * from before the wait began. Splitting the fast check and the fallback into two statements
     * means the fallback's statement starts (and takes its snapshot) only after any blocking in
     * the first has already been resolved.
     * <p>
     * The fallback treats any still-unfinalized event (negative {@code global_index}) as
     * unconditionally past the marker: a marker can never be past the last visible index, so an
     * unfinalized event - committed, just not yet renumbered - is always "later" than it, even
     * though it stays invisible to reads until finalized.
     * <p>
     * Because the fallback can grant permission to update even when the existing value is
     * negative (unlike the fast check alone, which only ever fires when it is {@code >= 0}), the
     * existing value is no longer guaranteed to be numerically greater than {@code
     * EXCLUDED.global_index}: a concurrently-processing event with a more negative (later) temp
     * index could complete its own locking first. {@code LEAST(...)} keeps whichever of the two
     * is actually more recent, so this can never regress a hash's recorded position - the same
     * reasoning as {@link #UNCONDITIONAL_CONSISTENCY_TAGS_UPSERT}.
     * <p>
     * Verified empirically against PostgreSQL 16: being uncorrelated to any individual row, the
     * fallback subquery is evaluated at most once per statement (hoisted into an {@code
     * InitPlan}) regardless of how many attribute hashes are being locked, and not at all when
     * the fast check alone resolves every one of them - so this costs nothing when uncontended,
     * and a single, flat check when it isn't.
     *
     * <li>Parameter group, repeated once per attribute hash: {@code int} the tag hash, {@code long} the temporary global index
     * <li>Parameter {@code long}: the marker position, for the fast check
     * <li>Parameter {@code long}: the marker position again, for the fallback check
     * <li>Parameters: the criterion's tag/type bind parameters, see {@link #buildCriterionSql(EventCriterion)}
     * <li>Final parameter {@code int}: the criterion's distinct tag count, for the fallback's {@code HAVING}
     */
    private static final String CONSISTENCY_TAGS_LOCK =
        """
        INSERT INTO consistency_tags (tag_hash, global_index) VALUES {values}
          ON CONFLICT (tag_hash) DO UPDATE
            SET global_index = LEAST(consistency_tags.global_index, EXCLUDED.global_index)
            WHERE (consistency_tags.global_index < ? AND consistency_tags.global_index >= 0)
               OR NOT EXISTS (
                 SELECT 1
                   FROM tags t
                   WHERE (t.global_index >= ? OR t.global_index < 0) AND ({condition})
                   GROUP BY t.global_index
                   HAVING COUNT(DISTINCT (t.key, t.value)) = ?
                   LIMIT 1
               )
          RETURNING tag_hash
        """;

    /**
     * Directly checks whether an event matching a criterion exists beyond a given marker, for
     * criteria with no tags at all (a pure type restriction) that have nothing to hash-lock
     * through {@link #CONSISTENCY_TAGS_LOCK} - see {@link #hasConflict}.
     * <p>
     * The {@code {condition}} place-holder must be replaced with the criterion's tag/type
     * condition - see {@link #buildCriterionSql(EventCriterion)}. Treats any still-unfinalized
     * event (negative {@code global_index}) as unconditionally past the marker, for the same
     * reason {@link #CONSISTENCY_TAGS_LOCK}'s fallback does.
     *
     * <li>Parameter {@code long}: the marker position
     * <li>Parameters: the criterion's tag/type bind parameters, see {@link #buildCriterionSql(EventCriterion)}
     * <li>Final parameter {@code int}: the criterion's distinct tag count, for the {@code HAVING}
     */
    private static final String CRITERION_CONFLICT_CHECK =
        """
        SELECT 1
          FROM tags t
          WHERE (t.global_index >= ? OR t.global_index < 0)
            AND ({condition})
          GROUP BY t.global_index
          HAVING COUNT(DISTINCT (t.key, t.value)) = ?
          LIMIT 1
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
     * <p>
     * Package-private so tests can assert this hasn't silently changed - a hardcoded hash
     * collision fixture is only valid for this exact capacity.
     */
    final int hashCapacity = 1024 * 1024;  // must be a power of 2

    /**
     * Derived from capacity (which must be a power of 2). Package-private for the same
     * reason as {@link #hashCapacity}.
     */
    final int hashMask = hashCapacity - 1;

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
            return finalizer.scheduleFinalization();
        }
    };

    /**
     * Watches for new events arriving via Postgres {@code LISTEN}/{@code NOTIFY} and notifies
     * registered stream callbacks.
     */
    private final PostgresqlEventMonitor eventMonitor;

    /**
     * Assigns permanent global indices to events appended with a temporary one, notifying
     * {@link #eventMonitor} of the resulting high-water mark once done.
     */
    private final PostgresqlFinalizer finalizer;

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

        try {
            PostgresqlSchemaInitializer.initialize(dataSource);
        }
        catch (SQLException e) {
            throw new IllegalStateException("Could not initialize " + getClass().getSimpleName(), e);
        }

        this.eventMonitor = new PostgresqlEventMonitor(dataSource);
        this.finalizer = new PostgresqlFinalizer(dataSource, eventMonitor::updateHighestKnownGlobalIndex);
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
    public CompletableFuture<Void> store(QualifiedName qualifiedName, Object identifier, Snapshot snapshot,
                                         @Nullable ProcessingContext context) {
        return snapshotStore.store(qualifiedName, identifier, snapshot, context);
    }

    @Override
    public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier,
                                                      @Nullable ProcessingContext context) {
        return snapshotStore.load(qualifiedName, identifier, context);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("dataSource", dataSource);
        descriptor.describeProperty("converter", converter);
        descriptor.describeProperty("transactionalExecutorProvider", transactionalExecutorProvider);
        descriptor.describeProperty("snapshotStore", snapshotStore);
    }

    @Override
    public CompletableFuture<AppendTransaction<?>> appendEvents(
        AppendCondition condition,
        ProcessingContext context,
        List<TaggedEventMessage<?>> events
    ) {
        validateNoReservedTags(events);

        entitlementManager.claimMessage(PostgresAxoniqAddon.IDENTIFIER, EntitlementMessageType.EVENT, events.size());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("appendEvents: called with condition=" + condition + ", events=" + events + ", context=" + context);
        }

        return connectionExecutor(context).apply(connection -> {
            if (!internalAppendEvents(connection, condition, events)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("appendEvents: failed");
                }

                Set<Tag> tags = condition.criteria()
                    .flatten()
                    .stream()
                    .flatMap(criterion -> criterion.tags().stream())
                    .collect(Collectors.toSet());

                throw AppendEventsTransactionRejectedException.conflictingEventsDetected(condition.consistencyMarker(), tags);  // allow executor to rollback correctly
            }

            return appendTransaction;
        });
    }

    /**
     * Rejects any event carrying a tag with the reserved {@link #TYPE_TAG_KEY key}, which is written
     * automatically by the {@code axon_write_type_tag} database trigger and must never be supplied
     * directly.
     *
     * @param events the events to validate, cannot be {@code null}
     * @throws IllegalArgumentException if any event carries a tag using the reserved key
     */
    private static void validateNoReservedTags(List<TaggedEventMessage<?>> events) {
        for (TaggedEventMessage<?> tem : events) {
            for (Tag tag : tem.tags()) {
                if (TYPE_TAG_KEY.equals(tag.key())) {
                    throw new IllegalArgumentException(
                        "Tag key \"" + TYPE_TAG_KEY + "\" is reserved for internal use and cannot be supplied explicitly"
                    );
                }
            }
        }
    }

    // TODO #8 performance improvement possible here by avoiding a lot of back-and-forth with the server
    private boolean internalAppendEvents(Connection connection, AppendCondition condition, List<TaggedEventMessage<?>> events) throws SQLException {
        try (
            PreparedStatement eventInsert = connection.prepareStatement(EVENTS_INSERT, Statement.RETURN_GENERATED_KEYS);
            PreparedStatement unconditionalConsistencyTagsUpsert = connection.prepareStatement(UNCONDITIONAL_CONSISTENCY_TAGS_UPSERT);
            PreparedStatement tagInsert = connection.prepareStatement(TAG_INSERT);
        ) {
            Set<Integer> batchLockedHashes = new HashSet<>();

            for (TaggedEventMessage<?> tem : events) {
                EventMessage message = tem.event();
                String eventTypeName = message.type().qualifiedName().fullName();

                eventInsert.setTimestamp(1, Timestamp.from(message.timestamp()));
                eventInsert.setString(2, message.identifier());
                eventInsert.setString(3, eventTypeName);
                eventInsert.setString(4, message.type().version());
                eventInsert.setBytes(5, converter.convertPayload(message, byte[].class));
                eventInsert.setString(6, MetadataSerializer.toJson(message.metadata()));
                eventInsert.execute();

                try (ResultSet keys = eventInsert.getGeneratedKeys()) {
                    if (!keys.next()) {
                        throw new IllegalStateException("Generated keys were expected. Please upgrade your JDBC driver.");
                    }

                    long temporaryGlobalIndex = keys.getLong(1);
                    Set<Integer> lockedHashes = lock(connection, condition, temporaryGlobalIndex, batchLockedHashes);

                    if (lockedHashes == null) {  // locking of some or all attributes failed, return that appending failed
                        return false;
                    }

                    batchLockedHashes.addAll(lockedHashes);

                    // Update unconditional tags (tags not part of the append condition):
                    for (Tag tag : tem.tags()) {
                        int tagHash = consistencyHash(tag.key(), tag.value());

                        if (!lockedHashes.contains(tagHash)) {
                            unconditionalConsistencyTagsUpsert.setInt(1, tagHash);
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
    public MessageStream<EventMessage> source(SourcingCondition condition, @Nullable ProcessingContext context) {
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

            /*
             * Repeatable read is used here because EVENTS_READ_MULTIPLE is two statements - the page
             * read and a trailing MAX(global_index) watermark - and under READ COMMITTED each would
             * take its own snapshot, letting a commit land in between and silently skip events. This
             * only affects this dedicated, single-call, read-only connection - not
             * CONSISTENCY_TAGS_LOCK's append-side connection, which relies on READ COMMITTED's
             * per-statement refresh instead.
             */

            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            try (PreparedStatement ps = connection.prepareStatement(query)) {
                int parameterIndex = 1;

                if (sourcingStrategy instanceof SourcingStrategy.Snapshot s) {
                    ps.setString(parameterIndex++, s.qualifiedName().fullName());
                    ps.setString(parameterIndex++, String.valueOf(s.identifier()));
                }

                ps.setLong(parameterIndex++, positionValue);  // RESUME_AT_POSITION's start position or RESUME_AT_SNAPSHOT's maximum position

                for (CriterionFilter criterionFilter : tagFilter.criterionFilters) {
                    for (String parameter : criterionFilter.tagParameters()) {
                        ps.setString(parameterIndex++, parameter);  // tag parameters in subquery
                    }

                    if (!criterionFilter.typeParameters().isEmpty()) {
                        ps.setArray(parameterIndex++, connection.createArrayOf("varchar", criterionFilter.typeParameters().toArray()));
                    }

                    ps.setInt(parameterIndex++, criterionFilter.distinctTagCount());  // HAVING COUNT in each subquery
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
        MessageType messageType = new MessageType(new QualifiedName(resultSet.getString(4)), resultSet.getString(9));
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

    /**
     * Builds the tag/type filter for the given criteria, unioning one {@link #FILTER_SUB_QUERY}
     * instance per criterion. A criterion with neither tags nor types restricts nothing, so the
     * whole filter collapses to {@link #EMPTY} (match everything) the moment one is encountered -
     * that is correct even with other, more restrictive criteria present, since criteria combine
     * with OR semantics.
     * <p>
     * Otherwise, each criterion's condition is one of:
     * <ul>
     *     <li>tags only - {@code (t.key, t.value) IN (...)}
     *     <li>types only - {@code t.key = '__T' AND t.value = ANY(?)}, no tags to join on at all
     *     <li>both - the two conditions above, combined with {@code OR}
     * </ul>
     *
     * @param criterions the criteria to filter events on, cannot be {@code null}
     * @return the resulting filter, never {@code null}
     */
    private static TagFilter buildTagFilter(Set<EventCriterion> criterions) {
        if (criterions.isEmpty()) {
            return EMPTY;
        }

        StringBuilder sql = new StringBuilder();
        List<CriterionFilter> criterionFilters = new ArrayList<>();
        boolean firstCriterion = true;

        for (EventCriterion criterion : criterions) {
            if (criterion.tags().isEmpty() && criterion.types().isEmpty()) {
                return EMPTY;  // no restriction at all means match all events, so no filter needed
            }

            if (!firstCriterion) {
                sql.append(" UNION ");
            }

            CriterionSql criterionSql = buildCriterionSql(criterion);

            criterionFilters.add(criterionSql.filter());
            sql.append(FILTER_SUB_QUERY.replace("{condition}", criterionSql.condition()));

            firstCriterion = false;
        }

        return new TagFilter(sql, criterionFilters);
    }

    /**
     * Builds the {@code condition} fragment and bind parameters for a single criterion, shared
     * by {@link #buildTagFilter(Set)} (which unions one instance per criterion for reads) and
     * {@link #lock} (which checks one criterion at a time for the write-side fallback).
     *
     * @param criterion the criterion to build a filter fragment for, cannot be {@code null}
     * @return the condition fragment and its bind parameters, never {@code null}
     */
    private static CriterionSql buildCriterionSql(EventCriterion criterion) {
        List<String> tagParameters = new ArrayList<>();
        StringBuilder keyValuePairs = new StringBuilder();

        for (Tag tag : criterion.tags()) {
            if (!keyValuePairs.isEmpty()) {
                keyValuePairs.append(", ");
            }

            keyValuePairs.append("(?, ?)");

            tagParameters.add(tag.key());
            tagParameters.add(tag.value());
        }

        List<String> typeParameters = criterion.types().stream().map(QualifiedName::fullName).toList();
        String typeCondition = "t.key = '" + TYPE_TAG_KEY + "' AND t.value = ANY(?)";
        String condition = keyValuePairs.isEmpty()
            ? typeCondition
            : typeParameters.isEmpty()
                ? "(t.key, t.value) IN (" + keyValuePairs + ")"
                : "(t.key, t.value) IN (" + keyValuePairs + ") OR (" + typeCondition + ")";

        int distinctTagCount = criterion.tags().size() + (typeParameters.isEmpty() ? 0 : 1);

        return new CriterionSql(condition, new CriterionFilter(tagParameters, typeParameters, distinctTagCount));
    }

    /**
     * This function locks every criterion of the given append condition, so other concurrent
     * events being appended using overlapping tags or types will block. If this transaction is
     * committed, any other transactions blocking on an overlapping attribute will fail. If this
     * transaction is rolled back, they may progress.
     * <p>
     * Each real tag is its own independent attribute hash, batched into a single {@link
     * #CONSISTENCY_TAGS_LOCK} statement per criterion, so one round trip locks all of a
     * criterion's tags at once. That statement's embedded fallback resolves any hash that looks
     * stale by precisely re-checking the criterion as a whole against the real data - tags and
     * type together, via {@link #buildCriterionSql(EventCriterion)} - so a shared tag with a
     * non-matching type, or an unrelated tag that merely collides on the same hash bucket,
     * doesn't cause a false conflict.
     * <p>
     * A type restriction is never itself hash-locked: unlike tags, types are typically
     * low-cardinality, so every event of a common type would contend on the same handful of
     * rows for no benefit - the fallback above already establishes type-precision once a tag's
     * fast check needs it, by reading the {@code __T} rows the {@code axon_write_type_tag}
     * trigger maintains directly. The only gap that leaves is a criterion with no tags at all
     * (a pure type restriction, nothing to hash-lock through), handled separately by directly
     * running that same precise check unconditionally - see {@link #hasConflict}.
     *
     * @param connection the connection to lock on, cannot be {@code null}
     * @param condition the append condition, cannot be {@code null}
     * @param temporaryGlobalIndex the (temporary) index of a newly inserted event, always negative
     * @param batchLockedHashes attribute hashes already locked in this batch, cannot be {@code null}
     * @return the attribute hashes that were locked (possibly empty), or {@code null} if locking failed
     * @throws SQLException when a JDBC error occurred
     */
    private Set<Integer> lock(Connection connection, AppendCondition condition, long temporaryGlobalIndex, Set<Integer> batchLockedHashes) throws SQLException {

        /*
         * The position in a consistency marker is the position just after the last event it was consistent with,
         * as this position is the position one can resume a sourcing from without having to adjust it manually.
         * The locking insert/update query used uses a less than comparison ("<"), so this will work correctly, without
         * having to adjust the marker position.
         */

        long globalIndex = Math.max(0, GlobalIndexConsistencyMarker.position(condition.consistencyMarker()));

        assert temporaryGlobalIndex < 0;
        assert globalIndex >= 0;

        Set<Integer> lockedHashes = new HashSet<>();

        for (EventCriterion criterion : condition.criteria().flatten()) {
            if (criterion.tags().isEmpty()) {  // a pure type restriction - nothing to hash-lock, check directly
                if (hasConflict(connection, criterion, globalIndex)) {
                    return null;
                }

                continue;
            }

            /*
             * Sorted (rather than e.g. insertion order) so that any two transactions locking an
             * overlapping set of hashes always attempt them in the same order - the standard fix
             * for ABBA deadlocks between concurrent multi-row lock attempts.
             */
            Set<Integer> hashesToLock = new TreeSet<>();

            for (Tag tag : criterion.tags()) {
                hashesToLock.add(consistencyHash(tag.key(), tag.value()));
            }

            hashesToLock.removeAll(batchLockedHashes);
            hashesToLock.removeAll(lockedHashes);

            if (hashesToLock.isEmpty()) {
                continue;  // every tag of this criterion was already locked earlier in this batch
            }

            Set<Integer> locked = lockCriterion(connection, criterion, hashesToLock, temporaryGlobalIndex, globalIndex);

            if (locked == null) {
                return null;
            }

            lockedHashes.addAll(locked);
        }

        return lockedHashes;
    }

    /**
     * Directly checks whether an event matching the given criterion exists beyond the given
     * marker, for criteria with no tags at all (a pure type restriction) that therefore have
     * nothing to hash-lock through {@link #CONSISTENCY_TAGS_LOCK} - see {@link #lock}.
     * <p>
     * Treats any still-unfinalized event (negative {@code global_index}) as unconditionally
     * past the marker, for the same reason {@link #CONSISTENCY_TAGS_LOCK}'s fallback does.
     *
     * @param connection the connection to check on, cannot be {@code null}
     * @param criterion the (tag-less) criterion to check, cannot be {@code null}
     * @param globalIndex the marker position to check consistency against, never negative
     * @return {@code true} if a matching event exists beyond the marker
     * @throws SQLException when a JDBC error occurred
     */
    private boolean hasConflict(Connection connection, EventCriterion criterion, long globalIndex) throws SQLException {
        CriterionSql criterionSql = buildCriterionSql(criterion);
        CriterionFilter filter = criterionSql.filter();

        String sql = CRITERION_CONFLICT_CHECK.replace("{condition}", criterionSql.condition());

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int parameterIndex = 1;

            ps.setLong(parameterIndex++, globalIndex);

            for (String parameter : filter.tagParameters()) {
                ps.setString(parameterIndex++, parameter);
            }

            if (!filter.typeParameters().isEmpty()) {
                ps.setArray(parameterIndex++, connection.createArrayOf("varchar", filter.typeParameters().toArray()));
            }

            ps.setInt(parameterIndex, filter.distinctTagCount());

            try (ResultSet resultSet = ps.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    /**
     * Locks every given attribute hash for a single criterion, in at most two round trips - see
     * {@link #lock}.
     * <p>
     * The first round trip ({@link #fastLock}) tries the cheap, same-row-only check for every
     * hash at once. If that alone resolves them all - the common case for high-cardinality tags -
     * this returns without a second round trip. Otherwise, a second, separate round trip ({@link
     * #fallbackLock}) re-checks only the hashes still unresolved, now backed by the precise
     * fallback. This second statement must never be merged into the first: see {@link
     * #CONSISTENCY_TAGS_LOCK}'s Javadoc for why a fallback sharing a statement with a blocking
     * write can observe a stale snapshot.
     *
     * @param connection the connection to lock on, cannot be {@code null}
     * @param criterion the criterion {@code hashesToLock} belongs to, used to build the fallback's condition, cannot be {@code null}
     * @param hashesToLock the distinct attribute hashes to lock, cannot be {@code null} or empty
     * @param temporaryGlobalIndex the (temporary) index of a newly inserted event, always negative
     * @param globalIndex the marker position to check consistency against, never negative
     * @return the hashes that were locked, or {@code null} if a real conflict was confirmed
     * @throws SQLException when a JDBC error occurred
     */
    private Set<Integer> lockCriterion(Connection connection, EventCriterion criterion, Set<Integer> hashesToLock, long temporaryGlobalIndex, long globalIndex) throws SQLException {
        Set<Integer> resolved = fastLock(connection, hashesToLock, temporaryGlobalIndex, globalIndex);

        if (resolved.size() == hashesToLock.size()) {
            return resolved;
        }

        Set<Integer> remaining = new TreeSet<>(hashesToLock);

        remaining.removeAll(resolved);

        Set<Integer> fallbackResolved = fallbackLock(connection, criterion, remaining, temporaryGlobalIndex, globalIndex);

        if (fallbackResolved == null) {
            return null;
        }

        Set<Integer> allResolved = new HashSet<>(resolved);

        allResolved.addAll(fallbackResolved);

        return allResolved;
    }

    /**
     * Attempts the cheap, same-row-only {@link #CONSISTENCY_TAGS_FAST_LOCK} for every given hash
     * in one round trip - the first phase of {@link #lockCriterion}.
     *
     * @param connection the connection to lock on, cannot be {@code null}
     * @param hashesToLock the distinct attribute hashes to attempt, cannot be {@code null} or empty
     * @param temporaryGlobalIndex the (temporary) index of a newly inserted event, always negative
     * @param globalIndex the marker position to check consistency against, never negative
     * @return the hashes that resolved via the fast check alone (possibly empty, never {@code null})
     * @throws SQLException when a JDBC error occurred
     */
    private Set<Integer> fastLock(Connection connection, Set<Integer> hashesToLock, long temporaryGlobalIndex, long globalIndex) throws SQLException {
        String values = String.join(", ", Collections.nCopies(hashesToLock.size(), "(?, ?)"));
        String sql = CONSISTENCY_TAGS_FAST_LOCK.replace("{values}", values);

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int parameterIndex = 1;

            for (int hash : hashesToLock) {
                ps.setInt(parameterIndex++, hash);
                ps.setLong(parameterIndex++, temporaryGlobalIndex);
            }

            ps.setLong(parameterIndex, globalIndex);

            try (ResultSet resultSet = ps.executeQuery()) {
                Set<Integer> lockedHashes = new HashSet<>();

                while (resultSet.next()) {
                    lockedHashes.add(resultSet.getInt(1));
                }

                return lockedHashes;
            }
        }
    }

    /**
     * Re-checks and locks the given (still-unresolved after {@link #fastLock}) attribute hashes
     * for a single criterion in one round trip, using {@link #CONSISTENCY_TAGS_LOCK}'s fast-check-
     * or-fallback logic - the second phase of {@link #lockCriterion}, run only when needed.
     *
     * @param connection the connection to lock on, cannot be {@code null}
     * @param criterion the criterion {@code hashesToLock} belongs to, used to build the fallback's condition, cannot be {@code null}
     * @param hashesToLock the distinct attribute hashes still needing resolution, cannot be {@code null} or empty
     * @param temporaryGlobalIndex the (temporary) index of a newly inserted event, always negative
     * @param globalIndex the marker position to check consistency against, never negative
     * @return the hashes that were locked, or {@code null} if the fallback confirmed a real conflict
     * @throws SQLException when a JDBC error occurred
     */
    private Set<Integer> fallbackLock(Connection connection, EventCriterion criterion, Set<Integer> hashesToLock, long temporaryGlobalIndex, long globalIndex) throws SQLException {
        CriterionSql criterionSql = buildCriterionSql(criterion);
        CriterionFilter filter = criterionSql.filter();

        String values = String.join(", ", Collections.nCopies(hashesToLock.size(), "(?, ?)"));
        String sql = CONSISTENCY_TAGS_LOCK
            .replace("{values}", values)
            .replace("{condition}", criterionSql.condition());

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int parameterIndex = 1;

            for (int hash : hashesToLock) {
                ps.setInt(parameterIndex++, hash);
                ps.setLong(parameterIndex++, temporaryGlobalIndex);
            }

            ps.setLong(parameterIndex++, globalIndex);  // fast check "< ?"
            ps.setLong(parameterIndex++, globalIndex);  // fallback ">= ?"

            for (String parameter : filter.tagParameters()) {
                ps.setString(parameterIndex++, parameter);
            }

            if (!filter.typeParameters().isEmpty()) {
                ps.setArray(parameterIndex++, connection.createArrayOf("varchar", filter.typeParameters().toArray()));
            }

            ps.setInt(parameterIndex, filter.distinctTagCount());

            try (ResultSet resultSet = ps.executeQuery()) {
                Set<Integer> lockedHashes = new HashSet<>();

                while (resultSet.next()) {
                    lockedHashes.add(resultSet.getInt(1));
                }

                return lockedHashes.size() == hashesToLock.size() ? lockedHashes : null;
            }
        }
    }

    /**
     * Computes the masked hash identifying a single tag-like attribute - a real tag, or the
     * reserved type restriction (see {@link #TYPE_TAG_KEY}) - as used for the {@code
     * consistency_tags.tag_hash} column.
     *
     * @param key   the tag (or reserved type) key, cannot be {@code null}
     * @param value the tag (or type) value, cannot be {@code null}
     * @return the masked hash, always non-negative
     */
    private int consistencyHash(String key, String value) {
        return hashPolicy.hash((key + ":" + value).getBytes(StandardCharsets.UTF_8)) & hashMask;
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
