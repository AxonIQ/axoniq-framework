/*
 * Copyright (c) 2010-2025. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */

package io.axoniq.framework.postgresql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventhandling.TerminalEventMessage;
import org.axonframework.eventhandling.conversion.EventConverter;
import org.axonframework.eventhandling.processors.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.eventhandling.processors.streaming.token.TrackingToken;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.StreamSpliterator;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventstreaming.EventCriterion;
import org.axonframework.eventstreaming.StreamingCondition;
import org.axonframework.eventstreaming.Tag;
import org.axonframework.messaging.Context;
import org.axonframework.messaging.MessageStream;
import org.axonframework.messaging.MessageType;
import org.axonframework.messaging.unitofwork.ProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException.conflictingEventsDetected;

/**
 * A {@link EventStorageEngine} implementation backed by PostgreSQL, providing
 * reliable event persistence and streaming with support for tagging.
 *
 * @author John Hendrikx
 * @since 0.1.0
 */
public final class PostgresqlEventStorageEngine implements EventStorageEngine {

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
     *
     * @param events the events returned as part of this batch, cannot be {@code null}, but may be empty
     * @param highestGlobalIndex the highest global index observed in this batch
     */
    private record Batch(List<FinalizedEvent> events, long highestGlobalIndex) {}

    private record TagFilter(String sql, List<String> tagParameters) {
        boolean isEmpty() {
            return tagParameters.isEmpty();
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresqlEventStorageEngine.class);
    private static final TagFilter EMPTY = new TagFilter("", List.of());
    private static final ExecutorService FINALIZER_EXECUTOR = Executors.newSingleThreadExecutor();  // must be a single thread
    private static final TypeReference<Map<String, String>> STRING_TO_STRING_MAP_TYPE_REFERENCE = new TypeReference<>() {};
    private static final GlobalSequenceTrackingToken GLOBAL_INDEX_START = new GlobalSequenceTrackingToken(1);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, false);

    /**
     * Queries events in order starting from a given global index, limited by the given limit.
     * Returns up to limit rows (including no rows at all if there were no matches). Also
     * Returns the highest known global index.
     *
     * <li>Parameter 1 {@code long}: global index to start querying from
     * <li>Parameter 2 {@code long}: maximum number of rows to query
     */
    private static final String EVENTS_READ_MULTIPLE =
        """
        SELECT global_index, timestamp, identifier, type, payload, metadata
          FROM events
          WHERE global_index >= ?
          ORDER BY global_index
          LIMIT ?;

        SELECT COALESCE(MAX(global_index), 0) AS last_seen
          FROM events;
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
     * <li>Parameter 6 {@code String}: the tags in JSON format
     */
    private static final String EVENTS_INSERT =
        """
        INSERT INTO events (timestamp, identifier, type, payload, metadata, tags)
          VALUES (?, ?, ?, ?, ?::json, ?::jsonb)
        """;

    /**
     * Upserts a tag with the latest global index, if it would be consistent, after inserting an event.
     *
     * <li>Parameter 1 {@code String}: the tag key
     * <li>Parameter 2 {@code String}: the tag value
     * <li>Parameter 3 {@code long}: the global index associated with the newly inserted event
     * <li>Parameter 4 {@code long}: the global index which it must be consistent with
     */
    private static final String CONSISTENCY_TAGS_UPSERT =
        """
        INSERT INTO consistency_tags (key, value, global_index) VALUES (?, ?, ?)
          ON CONFLICT (key, value) DO UPDATE
            SET global_index = EXCLUDED.global_index
            WHERE consistency_tags.global_index <= ? AND consistency_tags.global_index >= 0
        """;

    /**
     * Upserts a tag with the latest global index unconditionally after inserting an event.
     *
     * <li>Parameter 1 {@code String}: the tag key
     * <li>Parameter 2 {@code String}: the tag value
     * <li>Parameter 3 {@code long}: the global index associated with the newly inserted event
     */
    private static final String UNCONDITIONAL_CONSISTENCY_TAGS_UPSERT =
        """
        INSERT INTO consistency_tags (key, value, global_index) VALUES (?, ?, ?)
          ON CONFLICT (key, value) DO UPDATE
            SET global_index = LEAST(consistency_tags.global_index, EXCLUDED.global_index)
        """;

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
        finalized_consistency_tags AS (
          UPDATE consistency_tags t
          SET global_index = fe.new_val
          FROM finalized_events fe
          WHERE t.global_index = fe.old_val
          RETURNING fe.new_val
        )
        SELECT last_value FROM events_monotonic_seq;
        """;

    private final ConnectionExecutor connectionExecutor;
    private final EventConverter converter;

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
     * @param connectionExecutor a connection executor for running statements, cannot be {@code null}
     * @param converter an event converter for converting the payload to bytes, cannot be {@code null}
     */
    public PostgresqlEventStorageEngine(
        @Nonnull ConnectionExecutor connectionExecutor,
        @Nonnull EventConverter converter
    ) {
        this.connectionExecutor = Objects.requireNonNull(connectionExecutor, "connectionExecutor");
        this.converter = Objects.requireNonNull(converter, "converter");

        // TODO #7 Allow to configure tables, sequences and indices
        connectionExecutor.execute(null, connection -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                       global_index INT8 NOT NULL GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY -1),

                       timestamp TIMESTAMPTZ NOT NULL,
                       payload BYTEA,
                       metadata JSON NOT NULL,
                       identifier VARCHAR NOT NULL,
                       type VARCHAR NOT NULL,
                       tags JSONB NOT NULL,

                       -- keys
                       PRIMARY KEY (global_index),
                       UNIQUE (identifier)
                    );

                    CREATE TABLE IF NOT EXISTS consistency_tags (
                       key VARCHAR NOT NULL,
                       value VARCHAR NOT NULL,
                       global_index INT8 NOT NULL REFERENCES events(global_index),

                       -- keys
                       PRIMARY KEY (key, value)
                    );

                    -- Create a sequence used for monotonic final global index values. Starts at 1.
                    CREATE SEQUENCE IF NOT EXISTS events_monotonic_seq
                      INCREMENT BY 1
                      CACHE 1
                      OWNED BY events.global_index;

                    -- BRIN index on global_index
                    CREATE INDEX IF NOT EXISTS events_global_index_brin
                      ON events USING BRIN (global_index);

                    -- GIN index on tags (JSONB)
                    CREATE INDEX IF NOT EXISTS events_tags_gin
                      ON events USING GIN (tags);
                    """
                );
            }
        });
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        descriptor.describeProperty("connectionExecutor", connectionExecutor);
        descriptor.describeProperty("converter", converter);
    }

    @Override
    public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition, @Nullable ProcessingContext context, List<TaggedEventMessage<?>> events) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("appendEvents: called with condition=" + condition + ", events=" + events + ", context=" + context + "");
        }

        return CompletableFuture.completedFuture(new PgAppendTransaction(condition, events));
    }

    private class PgAppendTransaction implements AppendTransaction<Object> {
        final AppendCondition condition;
        final List<TaggedEventMessage<?>> events;

        PgAppendTransaction(AppendCondition condition, List<TaggedEventMessage<?>> events) {
            this.condition = condition;
            this.events = events;
        }

        @Override
        public CompletableFuture<Object> commit(ProcessingContext context) {
            try {
                connectionExecutor.execute(context, connection -> {
                    if (!internalAppendEvents(connection, condition, events)) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("appendEvents: failed");
                        }

                        throw conflictingEventsDetected(condition.consistencyMarker());  // allow executor to rollback correctly
                    }
                });

                return CompletableFuture.completedFuture(null);
            }
            catch (AppendEventsTransactionRejectedException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public void rollback(ProcessingContext context) {
            throw new UnsupportedOperationException();  // TODO #11 Implement rollback in AppendTransaction
        }

        @Override
        public CompletableFuture<ConsistencyMarker> afterCommit(Object commitResult, ProcessingContext context) {
            return scheduleFinalization();
        }
    }

    // TODO #8 performance improvement possible here by avoiding a lot of back-and-forth with the server
    private boolean internalAppendEvents(Connection connection, AppendCondition condition, List<TaggedEventMessage<?>> events) throws SQLException {
        try (
            PreparedStatement eventInsert = connection.prepareStatement(EVENTS_INSERT, Statement.RETURN_GENERATED_KEYS);
            PreparedStatement consistencyTagsUpsert = connection.prepareStatement(CONSISTENCY_TAGS_UPSERT);
            PreparedStatement unconditionalConsistencyTagsUpsert = connection.prepareStatement(UNCONDITIONAL_CONSISTENCY_TAGS_UPSERT);
        ) {
            for (TaggedEventMessage<?> tem : events) {
                EventMessage message = tem.event();
                ObjectNode tagsNode = JsonNodeFactory.instance.objectNode();

                tem.tags().forEach(tag -> tagsNode.put(tag.key(), tag.value()));

                eventInsert.setTimestamp(1, Timestamp.from(message.timestamp()));
                eventInsert.setString(2, message.identifier());
                eventInsert.setString(3, message.type().toString());
                eventInsert.setBytes(4, converter.convertPayload(message, byte[].class));
                eventInsert.setString(5, toString(message.metadata()));
                eventInsert.setString(6, tagsNode.toString());
                eventInsert.execute();

                try (ResultSet keys = eventInsert.getGeneratedKeys()) {
                    if (!keys.next()) {
                        throw new IllegalStateException("Generated keys were expected. Please upgrade your JDBC driver.");
                    }

                    long temporaryGlobalIndex = keys.getLong(1);
                    Set<Tag> lockedTags = lock(consistencyTagsUpsert, condition, temporaryGlobalIndex);

                    if (lockedTags == null) {  // locking of some or all tags failed, return that appending failed
                        return false;
                    }

                    // Update unconditional tags (tags not part of the append condition):
                    for (Tag tag : tem.tags()) {
                        if (!lockedTags.contains(tag)) {
                            unconditionalConsistencyTagsUpsert.setString(1, tag.key());
                            unconditionalConsistencyTagsUpsert.setString(2, tag.value());
                            unconditionalConsistencyTagsUpsert.setLong(3, temporaryGlobalIndex);
                            unconditionalConsistencyTagsUpsert.execute();
                        }
                    }
                }
            }

            return true;  // appending was successful
        }
    }

    @Override
    public MessageStream<EventMessage> source(@Nonnull SourcingCondition condition, @Nullable ProcessingContext context) {
        Set<EventCriterion> criterions = condition.criteria().flatten();
        CompletableFuture<Void> endOfStreams = new CompletableFuture<>();
        AtomicLong lastGlobalIndex = new AtomicLong();
        long start = Math.max(0, condition.start());

        return internalStream(context, criterions, start, lastGlobalIndex, List::isEmpty)
            .whenComplete(() -> endOfStreams.complete(null))
            .concatWith(MessageStream.fromFuture(
                endOfStreams.thenApply(event -> TerminalEventMessage.INSTANCE),
                unused -> Context.with(
                    ConsistencyMarker.RESOURCE_KEY,
                    new GlobalIndexConsistencyMarker(lastGlobalIndex.get())
                )
            ));
    }

    @Override
    public MessageStream<EventMessage> stream(@Nonnull StreamingCondition condition, @Nullable ProcessingContext context) {
        Set<EventCriterion> criterions = condition.criteria().flatten();
        TrackingToken trackingToken = condition.position();

        if (trackingToken != null && !(trackingToken instanceof GlobalSequenceTrackingToken)) {
            throw new IllegalArgumentException(
                "Tracking Token is not of expected type. Must be GlobalSequenceTrackingToken. Is: "
                    + trackingToken.getClass().getName()
            );
        }

        AtomicLong lastGlobalIndex = new AtomicLong();
        long start = trackingToken == null ? 0 : Math.max(0, trackingToken.position().orElse(1) - 1);

        return internalStream(context, criterions, start, lastGlobalIndex, batch -> false);
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken(@Nullable ProcessingContext context) {
        return CompletableFuture.completedFuture(GLOBAL_INDEX_START);
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken(@Nullable ProcessingContext context) {
        return connectionExecutor.execute(context, connection -> {
            try (
                PreparedStatement ps = connection.prepareStatement(EVENTS_FIND_NEXT_AVAILABLE_GLOBAL_INDEX);
                ResultSet resultSet = ps.executeQuery();
            ) {
                resultSet.next();

                long globalIndex = resultSet.getLong(1);

                return CompletableFuture.completedFuture(new GlobalSequenceTrackingToken(globalIndex));
            }
        });
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at, @Nullable ProcessingContext context) {
        return CompletableFuture.supplyAsync(() -> connectionExecutor.execute(context, connection -> {
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
        }));
    }

    private MessageStream<EventMessage> internalStream(
        ProcessingContext context,
        Set<EventCriterion> criterions,
        long start,
        AtomicLong lastGlobalIndex,
        Predicate<List<? extends FinalizedEvent>> predicate
    ) {
        StreamSpliterator<FinalizedEvent> entrySpliterator = new StreamSpliterator<>(
            last -> {
                long position = last == null ? start : last.position;
                Batch batch = load(context, criterions, position + 1, 50);

                lastGlobalIndex.set(batch.highestGlobalIndex);

                return batch.events;
            },
            predicate
        );

        return MessageStream.fromStream(
            StreamSupport.stream(entrySpliterator, false),
            FinalizedEvent::event,
            e -> TrackingToken.addToContext(Context.empty(), new GlobalSequenceTrackingToken(e.position + 1))
        );
    }

    // TODO #9 Prefetching via max parameter here should perhaps not be a concern of the engine
    private Batch load(ProcessingContext context, Set<EventCriterion> criterions, long position, int limit) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("load: loading from " + position + " (limit " + limit + ") with condition " + criterions + " and context " + context);
        }

        TagFilter tagFilter = buildTagFilter(criterions);
        String query = tagFilter.isEmpty()
            ? EVENTS_READ_MULTIPLE
            : EVENTS_READ_MULTIPLE.replace("WHERE global_index >= ?", "WHERE global_index >= ? AND " + tagFilter.sql);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("load: using query: " + query + " from filter: " + tagFilter);
        }

        return connectionExecutor.execute(context, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                int parameterIndex = 1;

                ps.setLong(parameterIndex++, position);  // global index parameter

                for (String parameter : tagFilter.tagParameters) {
                    ps.setString(parameterIndex++, parameter);  // tag parameters
                }

                ps.setLong(parameterIndex++, limit);

                if (!ps.execute()) {
                    throw new IllegalStateException("A ResultSet is expected");
                }

                List<FinalizedEvent> list = new ArrayList<>();

                try (ResultSet resultSet = ps.getResultSet()) {
                    while (resultSet.next()) {
                        long globalIndex = resultSet.getLong(1);
                        Instant timestamp = resultSet.getTimestamp(2).toInstant();
                        String identifier = resultSet.getString(3);
                        MessageType messageType = MessageType.fromString(resultSet.getString(4));
                        byte[] payload = resultSet.getBytes(5);
                        Map<String, String> metadata = toMetadata(resultSet.getString(6));

                        list.add(new FinalizedEvent(
                            globalIndex,
                            new GenericEventMessage(identifier, messageType, payload, metadata, timestamp)
                        ));
                    }
                }

                if (list.size() == limit) {

                    /*
                     * If limit was reached, the maximum global index query is not yet needed
                     * as there will be more queries needed to finish the stream. Don't
                     * bother fetching it and just return early:
                     */

                    return new Batch(list, list.getLast().position);
                }

                if (!ps.getMoreResults()) {
                    throw new IllegalStateException("A second ResultSet is expected");
                }

                try (ResultSet resultSet = ps.getResultSet()) {
                    resultSet.next();

                    long maxGlobalIndex = resultSet.getLong(1);

                    return new Batch(list, maxGlobalIndex);
                }
            }
        });
    }

    private static TagFilter buildTagFilter(Set<EventCriterion> criterions) {
        if (criterions.isEmpty()) {
            return EMPTY;
        }

        List<String> tagParameters = criterions.stream()
            .flatMap(criterion -> criterion.tags().stream()
                .map(tag -> JsonNodeFactory.instance.objectNode().put(tag.key(), tag.value()).toString())
            )
            .toList();

        String sql = criterions.stream()
            .map(criterion -> criterion.tags().stream()
                .map(tag ->"tags @> ?::jsonb")
                .collect(Collectors.joining(" AND "))
            )
            .collect(Collectors.joining(" OR ", "(", ")"));

        return new TagFilter(sql, tagParameters);
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
     * @return a set of tags that were locked (possibly empty), or {@code null} if locking failed
     * @throws SQLException when a JDBC error occurred
     */
    private Set<Tag> lock(PreparedStatement tagUpsert, AppendCondition condition, long temporaryGlobalIndex) throws SQLException {
        long globalIndex = Math.max(0, GlobalIndexConsistencyMarker.position(condition.consistencyMarker()));

        assert temporaryGlobalIndex < 0;
        assert globalIndex >= 0;

        Set<Tag> lockedTags = new HashSet<>();

        for(EventCriterion criterion : condition.criteria().flatten()) {
            // TODO #10 Support type based append transactions
            for (Tag tag : criterion.tags()) {
                tagUpsert.setString(1, tag.key());
                tagUpsert.setString(2, tag.value());
                tagUpsert.setLong(3, temporaryGlobalIndex);  // the temporary global index to write
                tagUpsert.setLong(4, globalIndex);  // the (permanent) global index to check for consistency (never negative)

                if (tagUpsert.executeUpdate() == 0) {
                    return null;
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
            return applyFinalization();
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

    private ConsistencyMarker applyFinalization() {
        return connectionExecutor.execute(null, connection -> {

            /*
             * Finalization is completely independent of any other transactions, and
             * the received connection therefore can be modified to suit finalization
             * needs.
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

                long latestGlobalIndex = resultSet.getLong(1);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("finalizePositions completed with latest global index: " + latestGlobalIndex);
                }

                return new GlobalIndexConsistencyMarker(latestGlobalIndex);
            }
        });
    }

    private static Map<String, String> toMetadata(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, STRING_TO_STRING_MAP_TYPE_REFERENCE);
        }
        catch (JsonProcessingException e) {  // should never occur
            throw new IllegalStateException(e);
        }
    }

    private static String toString(Map<String, String> metadata) {
        try {
            return OBJECT_MAPPER.writeValueAsString(metadata);
        }
        catch (JsonProcessingException e) {  // should never occur
            throw new IllegalStateException(e);
        }
    }
}
