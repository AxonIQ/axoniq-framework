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

import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import javax.sql.DataSource;

/**
 * Assigns permanent, monotonically increasing global indices to events appended with a temporary
 * (negative) index, coalescing concurrent finalization requests into as few runs as possible, and
 * reporting the resulting high-water mark to a callback.
 * <p>
 * Used internally by {@link PostgresqlEventStorageEngine}, which owns the single instance backing
 * its {@code AppendTransaction#afterCommit} hook, and supplies the callback that forwards the
 * finalized high-water mark to its {@link PostgresqlEventMonitor}.
 *
 * @author John Hendrikx
 * @since 5.2.0
 */
@Internal
final class PostgresqlFinalizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresqlFinalizer.class);

    /*
     * SELECT pg_advisory_xact_lock(42) must be its own statement, not a WITH clause: PostgreSQL
     * silently prunes an unreferenced SELECT CTE from the plan, so the lock would never be
     * acquired. Sending it together with the query below, in the same round trip and transaction,
     * still releases it automatically on commit or rollback.
     *
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
     *
     * consistency_tags is the only table here that a concurrently-running append transaction can also
     * be touching (events and tags are only ever touched again by the one transaction that inserted
     * them, then once, here, by whichever finalizer run picks them up - never by another transaction
     * in between). PostgresqlEventStorageEngine's own consistency_tags writes always acquire their row
     * locks in ascending tag_hash order (see its hashesToLock TreeSet) specifically to avoid ABBA
     * deadlocks between concurrent appends; for that same guarantee to hold against this statement too,
     * consistency_tags_ordered_lock takes the affected rows' locks in that same ascending tag_hash
     * order before finalized_consistency_tags writes them. UPDATE has no ORDER BY of its own, and an
     * ORDER BY inside a plain FROM-subquery is not guaranteed to control lock acquisition order (the
     * planner is free to join however it likes) - SELECT ... ORDER BY ... FOR UPDATE is the one
     * construct Postgres actually documents as locking rows in the order they arrive at its LockRows
     * node, which is why the ordering step needs to be its own, explicit SELECT rather than folded
     * into finalized_consistency_tags directly. It is marked MATERIALIZED so the planner can never
     * inline it back into the surrounding query and lose that guarantee.
     */
    private static final String FINALIZE_STATEMENT =
        """
        SELECT pg_advisory_xact_lock(42);

        WITH unfinalized_events AS (
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
        consistency_tags_ordered_lock AS MATERIALIZED (
          SELECT ct.tag_hash, fe.new_val
            FROM consistency_tags ct
            JOIN finalized_events fe ON ct.global_index = fe.old_val
            ORDER BY ct.tag_hash
            FOR UPDATE OF ct
        ),
        finalized_consistency_tags AS (
          UPDATE consistency_tags ct
          SET global_index = otl.new_val
          FROM consistency_tags_ordered_lock otl
          WHERE ct.tag_hash = otl.tag_hash
          RETURNING otl.new_val
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

    private final DataSource dataSource;
    private final LongConsumer onFinalized;
    private final ExecutorService finalizerExecutor =
            Executors.newSingleThreadExecutor(Thread.ofVirtual().name("PG-Finalizer").factory());  // must be a single thread

    /**
     * Synchronized field (via {@code this}). Future for the queued finalization which append
     * transactions can return from their {@code afterCommit} hook.
     */
    private CompletableFuture<ConsistencyMarker> queuedFinalization;

    /**
     * Synchronized field (via {@code this}). Tracks whether any finalizer is running currently.
     */
    private boolean finalizerRunning;

    /**
     * Constructs a new instance.
     *
     * @param dataSource  a data source to connect to PostgreSQL, cannot be {@code null}
     * @param onFinalized called with the latest known global index after each finalization run, cannot be {@code null}
     */
    PostgresqlFinalizer(DataSource dataSource, LongConsumer onFinalized) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.onFinalized = Objects.requireNonNull(onFinalized, "onFinalized");
    }

    synchronized CompletableFuture<ConsistencyMarker> scheduleFinalization() {
        if (!finalizerRunning) {
            finalizerRunning = true;

            return CompletableFuture.supplyAsync(this::runFinalizationTask, finalizerExecutor);
        }

        if (queuedFinalization == null) {
            queuedFinalization = CompletableFuture.supplyAsync(this::runFinalizationTask, finalizerExecutor);
        }

        return queuedFinalization;
    }

    void close() {  // for testing purposes, to avoid junk exceptions
        finalizerExecutor.shutdown();

        try {
            if (!finalizerExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                finalizerExecutor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            finalizerExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private ConsistencyMarker runFinalizationTask() {
        try {
            long latestGlobalIndex = finalizeAndReturnLatestIndex();

            onFinalized.accept(latestGlobalIndex);  // will notify callbacks if needed

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

            try (PreparedStatement ps = connection.prepareStatement(FINALIZE_STATEMENT)) {
                if (!ps.execute()) {
                    throw new IllegalStateException("A ResultSet is expected");
                }

                try (ResultSet lockResult = ps.getResultSet()) {
                    lockResult.next();  // consumes the lock statement's single (void) row
                }

                if (!ps.getMoreResults()) {
                    throw new IllegalStateException("A second ResultSet is expected");
                }

                try (ResultSet resultSet = ps.getResultSet()) {
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
    }
}
