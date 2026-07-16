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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.axonframework.common.jdbc.ConnectionExecutor;
import org.axonframework.common.jdbc.ConnectionProvider;
import org.axonframework.conversion.CachingSupplier;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStoreTestSuite;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

public class PostgresStorageEngineBackedEventStoreIT extends StorageEngineBackedEventStoreTestSuite<PostgresqlEventStorageEngine> {
    private static final UnitOfWorkFactory FACTORY = new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE);

    private static PostgreSQLContainer postgresContainer;
    private static DataSource dataSource;

    private static PostgresqlEventStorageEngine engine;
    private static EventConverter eventConverter;

    @SuppressWarnings("resource")
    @BeforeAll
    static void buildEngine() {
        postgresContainer = new PostgreSQLContainer("postgres:16.2")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test");

        postgresContainer.start();

        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(postgresContainer.getJdbcUrl());
        config.setUsername(postgresContainer.getUsername());
        config.setPassword(postgresContainer.getPassword());
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(1);
        config.setAutoCommit(false);

        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void stopContainer() {
        if (engine != null) {
            engine.close();
        }

        if (postgresContainer != null) {
            postgresContainer.stop();
        }
    }

    @Nested
    class OverrideAppendCondition
            extends StorageEngineBackedEventStoreTestSuite<PostgresqlEventStorageEngine>.OverrideAppendCondition {

        @Override
        @Disabled("Type based Append conditions not yet implemented")
        @Test
        protected void narrowedCriteriaShouldAvoidFalseConflict() {
            // TODO enable once #52 is fixed
        }
    }

    /**
     * Verifies {@link SourcingStrategy.Snapshot} sourcing end to end, through {@link
     * EventStoreTransaction#source(SourcingCondition)} rather than the engine directly - this
     * exercises the full single-round-trip snapshot query in {@link
     * PostgresqlEventStorageEngine#source(SourcingCondition)}, including the leading {@link
     * SnapshotEventMessage} and the pagination continuation for more events than fit in one
     * internal page (50).
     */
    @Nested
    class SnapshotSourcing {

        private QualifiedName qualifiedName;
        private String identifier;

        @BeforeEach
        void setUp() {
            qualifiedName = new QualifiedName(SnapshotSourcing.class);
            identifier = UUID.randomUUID().toString();
        }

        @Test
        void sourcingWithOnlyASnapshotReturnsJustTheSnapshot() {
            Snapshot snapshot = storeSnapshot(0);

            SnapshotSourcingResult result = sourceWithSnapshot();

            assertSnapshotMatches(result.snapshot(), snapshot);
            assertThat(result.events()).isEmpty();
        }

        @Test
        void sourcingWithASnapshotAndAFewEventsReturnsBoth() {
            Snapshot snapshot = storeSnapshot(currentPosition());

            EventMessage event1 = message(new SnapshotTestEvent(TAG1.value(), "1"));
            EventMessage event2 = message(new SnapshotTestEvent(TAG1.value(), "2"));
            EventMessage event3 = message(new SnapshotTestEvent(TAG1.value(), "3"));

            unitOfWork().executeWithResult(pc -> eventStore.publish(pc, event1, event2, event3)).join();

            SnapshotSourcingResult result = sourceWithSnapshot();

            assertSnapshotMatches(result.snapshot(), snapshot);
            assertThat(result.events())
                .usingComparatorForType(EVENT_COMPARATOR, GenericEventMessage.class)
                .containsExactly(event1, event2, event3);
        }

        @Test
        void sourcingWithASnapshotAndManyEventsContinuesBeyondFirstInternalPage() {
            Snapshot snapshot = storeSnapshot(currentPosition());

            // The engine internally pages in batches of 50 - use more than that to verify the
            // continuation after the initial snapshot-aware batch picks up every remaining
            // event, with no gaps or duplicates.
            EventMessage[] events = new EventMessage[550];

            for (int i = 0; i < events.length; i++) {
                events[i] = message(new SnapshotTestEvent(TAG1.value(), "event-" + i));
            }

            unitOfWork().executeWithResult(pc -> eventStore.publish(pc, events)).join();

            SnapshotSourcingResult result = sourceWithSnapshot();

            assertSnapshotMatches(result.snapshot(), snapshot);
            assertThat(result.events())
                .usingComparatorForType(EVENT_COMPARATOR, GenericEventMessage.class)
                .containsExactly(events);
        }

        private long currentPosition() {
            return unitOfWork().executeWithResult(eventStore::latestToken).join().position().orElse(0);
        }

        private Snapshot storeSnapshot(long position) {
            Snapshot snapshot = new Snapshot(new GlobalIndexPosition(position), "0.0.1", "snapshot-payload", Instant.now(), Map.of());

            engine.store(qualifiedName, identifier, snapshot, null).join();

            return snapshot;
        }

        /**
         * Compares {@code actual} against {@code expected} field by field rather than via {@code
         * equals()}, since two things never round-trip byte-for-byte through Postgres: the engine
         * always returns the snapshot payload as the raw, still-serialized {@code byte[]} (payload
         * deserialization is the caller's responsibility, not the engine's - see {@code
         * SnapshottingEntityLifecycleHandler#convertSnapshotPayload}), and {@code TIMESTAMPTZ} only
         * has microsecond precision, truncating part of {@link Instant#now()}'s nanoseconds.
         */
        private void assertSnapshotMatches(@Nullable Snapshot actual, Snapshot expected) {
            assertThat(actual).isNotNull();
            assertThat(actual.position()).isEqualTo(expected.position());
            assertThat(actual.version()).isEqualTo(expected.version());
            assertThat(actual.metadata()).isEqualTo(expected.metadata());
            assertThat(actual.timestamp()).isCloseTo(expected.timestamp(), within(1, ChronoUnit.MILLIS));
            assertThat(eventConverter.convert(actual.payload(), String.class)).isEqualTo(expected.payload());
        }

        /**
         * Sources with the {@link SourcingStrategy.Snapshot} strategy and splits the result into
         * the leading snapshot (if any) and the tail events, since the generic {@code source(...)}
         * helper on the base suite assumes every entry converts to {@code CourseUpdated}, which a
         * leading {@link SnapshotEventMessage} does not.
         */
        private SnapshotSourcingResult sourceWithSnapshot() {
            SourcingCondition condition = SourcingCondition.conditionFor(
                new SourcingStrategy.Snapshot(qualifiedName, identifier, null),
                EventCriteria.havingTags(TAG1)
            );

            return unitOfWork().executeWithResult(pc -> {
                EventStoreTransaction tx = eventStore.transaction(pc);
                MessageStream<? extends EventMessage> stream = tx.source(condition);
                AtomicReference<Snapshot> snapshotRef = new AtomicReference<>();

                return stream.reduce(new ArrayList<EventMessage>(), (list, entry) -> {
                    EventMessage message = entry.message();

                    if (message instanceof SnapshotEventMessage snapshotEventMessage) {
                        snapshotRef.set(snapshotEventMessage.payload());
                    }
                    else {
                        list.add(message.withConvertedPayload(SnapshotTestEvent.class, eventConverter));
                    }

                    return list;
                }).thenApply(events -> new SnapshotSourcingResult(snapshotRef.get(), events));
            }).join();
        }

        private record SnapshotSourcingResult(@Nullable Snapshot snapshot, List<EventMessage> events) {}

        @Event
        record SnapshotTestEvent(@EventTag(key = "Course") String id, String data) {}
    }

    @Override
    protected PostgresqlEventStorageEngine getStorageEngine(EventConverter converter) {
        eventConverter = converter;

        if (engine == null) {
            engine = new PostgresqlEventStorageEngine(dataSource, converter);
        }

        return engine;
    }

    @Override
    protected UnitOfWork unitOfWork() {
        UnitOfWork unitOfWork = FACTORY.create();

        var cp = new ConnectionProvider() {
            private Connection obtainedConnection;

            @Override
            public synchronized Connection getConnection() throws SQLException {
                if (obtainedConnection == null) {
                    obtainedConnection = dataSource.getConnection();

                    obtainedConnection.setAutoCommit(false);
                }

                return obtainedConnection;
            }

            synchronized void commit() throws SQLException {
                if (obtainedConnection != null) {
                    obtainedConnection.commit();
                }
            }

            synchronized void rollback() throws SQLException {
                if (obtainedConnection != null) {
                    obtainedConnection.rollback();
                }
            }

            synchronized void close() {
                try {
                    if (obtainedConnection != null) {
                        obtainedConnection.close();
                    }
                }
                catch (SQLException e) {
                    // Ignore, if closing fails, the system is likely in a bad state already
                }
            }
        };

        unitOfWork.runOnPreInvocation(pc -> {
            pc.putResource(
                JdbcTransactionalExecutorProvider.SUPPLIER_KEY,
                CachingSupplier.of(() -> new ConnectionExecutor(cp))
            );

            pc.onCommit(p -> {
                try {
                    cp.commit();

                    return CompletableFuture.completedFuture(null);
                }
                catch (SQLException e) {
                    return CompletableFuture.failedFuture(e);
                }
                finally {
                    cp.close();
                }
            });

            pc.onError((p, phase, e) -> {
                try {
                    cp.rollback();
                }
                catch (SQLException se) {
                    e.addSuppressed(se);
                }
                finally {
                    cp.close();
                }
            });
        });

        return unitOfWork;
    }

}
