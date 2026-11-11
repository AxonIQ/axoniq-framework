/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.axonframework.common.function.ThrowingFunction;
import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AggregateSequenceNumberPosition;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Test class validating the {@link PostgresqlSnapshotStore}.
 *
 * @author John Hendrikx
 */
class PostgresqlSnapshotStoreTest {

    private static final QualifiedName QUALIFIED_NAME = new QualifiedName("com.example.MyAggregate");
    private static final String IDENTIFIER = "aggregate-1";
    // Truncated to milliseconds to survive TIMESTAMPTZ round-trip (microsecond precision)
    private static final Instant TIMESTAMP = Instant.parse("2026-01-01T12:00:00Z");

    private static PostgreSQLContainer postgresContainer;
    private static HikariDataSource dataSource;
    private static PostgresqlSnapshotStore testSubject;

    @BeforeAll
    @SuppressWarnings("resource")
    static void startContainer() throws SQLException {
        postgresContainer = new PostgreSQLContainer("postgres:16.2-alpine")
                .withDatabaseName("testdb")
                .withUsername("test")
                .withPassword("test");
        postgresContainer.start();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgresContainer.getJdbcUrl());
        config.setUsername(postgresContainer.getUsername());
        config.setPassword(postgresContainer.getPassword());
        config.setAutoCommit(false);

        dataSource = new HikariDataSource(config);
        PostgresqlSchemaInitializer.initialize(dataSource, SchemaInitialization.CREATE_IF_MISSING);
        testSubject = new PostgresqlSnapshotStore(dataSource, new JacksonConverter(),
                                                   new JdbcTransactionalExecutorProvider(dataSource));
    }

    @AfterAll
    static void stopContainer() {
        dataSource.close();

        if (postgresContainer != null) {
            postgresContainer.stop();
        }
    }

    @BeforeEach
    void clearSnapshots() throws SQLException {
        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement()
        ) {
            statement.execute("DELETE FROM snapshots");
            connection.commit();
        }
    }

    @Nested
    class Load {

        @Test
        void load_withNoSnapshot_returnsNull() {
            // when
            Snapshot result = testSubject.load(QUALIFIED_NAME, IDENTIFIER, null)
                                         .orTimeout(5, TimeUnit.SECONDS)
                                         .join();

            // then
            assertThat(result).isNull();
        }

        @Test
        void load_forDifferentIdentifier_returnsNull() {
            // given
            testSubject.store(QUALIFIED_NAME, IDENTIFIER, snapshot(42, "1.0"), null)
                       .orTimeout(5, TimeUnit.SECONDS).join();

            // when
            Snapshot result = testSubject.load(QUALIFIED_NAME, "other-identifier", null)
                                         .orTimeout(5, TimeUnit.SECONDS)
                                         .join();

            // then
            assertThat(result).isNull();
        }

        @Test
        void load_forDifferentQualifiedName_returnsNull() {
            // given
            testSubject.store(QUALIFIED_NAME, IDENTIFIER, snapshot(42, "1.0"), null)
                       .orTimeout(5, TimeUnit.SECONDS).join();

            // when
            Snapshot result = testSubject.load(new QualifiedName("com.example.OtherAggregate"), IDENTIFIER, null)
                                         .orTimeout(5, TimeUnit.SECONDS)
                                         .join();

            // then
            assertThat(result).isNull();
        }
    }

    @Nested
    class Store {

        @Test
        void store_then_load_roundtripsAllFields() {
            // given
            byte[] payload = "test-payload".getBytes();
            Map<String, String> metadata = Map.of("key", "value");
            Snapshot snapshot = new Snapshot(new GlobalIndexPosition(42), "1.0", payload, TIMESTAMP, metadata);

            // when
            testSubject.store(QUALIFIED_NAME, IDENTIFIER, snapshot, null).orTimeout(5, TimeUnit.SECONDS).join();
            Snapshot loaded = testSubject.load(QUALIFIED_NAME, IDENTIFIER, null).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(loaded).isNotNull();
            assertThat(loaded.position()).isEqualTo(new GlobalIndexPosition(42));
            assertThat(loaded.version()).isEqualTo("1.0");
            assertThat(loaded.payload()).isEqualTo(payload);
            assertThat(loaded.timestamp()).isEqualTo(TIMESTAMP);
            assertThat(loaded.metadata()).isEqualTo(metadata);
        }

        @Test
        void store_replacesSnapshotWithDifferentVersion() {
            // given
            testSubject.store(QUALIFIED_NAME, IDENTIFIER, snapshot(10, "1.0"), null)
                       .orTimeout(5, TimeUnit.SECONDS).join();

            // when
            byte[] newPayload = "new-payload".getBytes();
            testSubject.store(QUALIFIED_NAME, IDENTIFIER, new Snapshot(new GlobalIndexPosition(20), "2.0", newPayload, TIMESTAMP, Map.of()), null)
                       .orTimeout(5, TimeUnit.SECONDS).join();
            Snapshot loaded = testSubject.load(QUALIFIED_NAME, IDENTIFIER, null).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(loaded).isNotNull();
            assertThat(loaded.position()).isEqualTo(new GlobalIndexPosition(20));
            assertThat(loaded.version()).isEqualTo("2.0");
            assertThat(loaded.payload()).isEqualTo(newPayload);
        }

        @Test
        void store_replacesSnapshotWithSameVersion() {
            // given
            testSubject.store(QUALIFIED_NAME, IDENTIFIER, snapshot(10, "1.0"), null)
                       .orTimeout(5, TimeUnit.SECONDS).join();

            // when
            byte[] updatedPayload = "updated-payload".getBytes();
            testSubject.store(QUALIFIED_NAME, IDENTIFIER, new Snapshot(new GlobalIndexPosition(10), "1.0", updatedPayload, TIMESTAMP, Map.of()), null)
                       .orTimeout(5, TimeUnit.SECONDS).join();
            Snapshot loaded = testSubject.load(QUALIFIED_NAME, IDENTIFIER, null).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(loaded).isNotNull();
            assertThat(loaded.payload()).isEqualTo(updatedPayload);
        }

        @Test
        void store_withUnsupportedPositionType_throwsImmediately() {
            // given
            Snapshot snapshot = new Snapshot(new AggregateSequenceNumberPosition(5), "1.0", "payload".getBytes(), TIMESTAMP, Map.of());

            // when / then — must throw synchronously, before any async work
            assertThatThrownBy(() -> testSubject.store(QUALIFIED_NAME, IDENTIFIER, snapshot, null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unsupported position type");
        }
    }

    @Nested
    @Timeout(60)
    class StoreInTheBackground {

        private HikariDataSource singleConnectionPool;
        private PostgresqlSnapshotStore backgroundSubject;

        @BeforeEach
        void setUp() {
            HikariConfig config = new HikariConfig();

            config.setJdbcUrl(postgresContainer.getJdbcUrl());
            config.setUsername(postgresContainer.getUsername());
            config.setPassword(postgresContainer.getPassword());
            config.setAutoCommit(false);
            config.setMaximumPoolSize(1);
            config.setMinimumIdle(1);
            config.setConnectionTimeout(60_000);

            singleConnectionPool = new HikariDataSource(config);
            backgroundSubject = new PostgresqlSnapshotStore(
                    singleConnectionPool, new JacksonConverter(), new JdbcTransactionalExecutorProvider(singleConnectionPool)
            );
        }

        @AfterEach
        void tearDown() {
            backgroundSubject.close();
            singleConnectionPool.close();
        }

        @Test
        void store_returnsWithoutWaitingForAConnection_andWritesWhenOneIsAvailable() throws SQLException {
            // given
            Connection held = singleConnectionPool.getConnection();

            // when
            CompletableFuture<Void> result =
                    backgroundSubject.store(QUALIFIED_NAME, IDENTIFIER, snapshot(42, "1.0"), null);

            // then
            assertThat(result).isNotDone();

            held.close();

            assertThat(result).succeedsWithin(Duration.ofSeconds(10));
            assertThat(testSubject.load(QUALIFIED_NAME, IDENTIFIER, null).orTimeout(5, TimeUnit.SECONDS).join())
                    .isNotNull();
        }

        @Test
        void store_isRejectedWhenTooManySnapshotsAreWaiting() throws SQLException {
            // given
            Connection held = singleConnectionPool.getConnection();
            List<CompletableFuture<Void>> accepted = new ArrayList<>();

            accepted.add(backgroundSubject.store(QUALIFIED_NAME, "entity-0", snapshot(1, "1.0"), null));

            // the writer has taken the first snapshot, and waits for the connection that is held
            await().atMost(10, TimeUnit.SECONDS)
                   .until(() -> singleConnectionPool.getHikariPoolMXBean().getThreadsAwaitingConnection() == 1);

            for (int i = 1; i <= PostgresqlSnapshotStore.WRITE_QUEUE_CAPACITY; i++) {
                accepted.add(backgroundSubject.store(QUALIFIED_NAME, "entity-" + i, snapshot(1, "1.0"), null));
            }

            // when
            CompletableFuture<Void> rejected =
                    backgroundSubject.store(QUALIFIED_NAME, "one-too-many", snapshot(1, "1.0"), null);

            // then
            assertThat(rejected)
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(RejectedExecutionException.class);

            held.close();

            CompletableFuture.allOf(accepted.toArray(CompletableFuture[]::new)).orTimeout(30, TimeUnit.SECONDS).join();

            assertThat(countSnapshots()).isEqualTo(PostgresqlSnapshotStore.WRITE_QUEUE_CAPACITY + 1);
        }

        @Test
        void store_isRejectedAfterTheStoreWasClosed() {
            // given
            backgroundSubject.close();

            // when
            CompletableFuture<Void> result =
                    backgroundSubject.store(QUALIFIED_NAME, IDENTIFIER, snapshot(42, "1.0"), null);

            // then
            assertThat(result)
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(RejectedExecutionException.class);
        }

        @Test
        void store_reportsAFailedWriteThroughTheFuture() {
            // given
            singleConnectionPool.close();

            // when
            CompletableFuture<Void> result =
                    backgroundSubject.store(QUALIFIED_NAME, IDENTIFIER, snapshot(42, "1.0"), null);

            // then
            assertThat(result)
                    .failsWithin(Duration.ofSeconds(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(SQLException.class);
        }

        @Test
        void store_keepsTheOldSnapshotWhenReplacingItFails() {
            // given a snapshot that is stored
            assertThat(backgroundSubject.store(QUALIFIED_NAME, IDENTIFIER, snapshot(42, "1.0"), null))
                    .succeedsWithin(Duration.ofSeconds(10));

            // when its replacement fails to insert, after the old one was deleted in the same transaction
            CompletableFuture<Void> replacement =
                    backgroundSubject.store(QUALIFIED_NAME, IDENTIFIER, snapshot(43, "bad\u0000version"), null);

            // then
            assertThat(replacement).failsWithin(Duration.ofSeconds(10));

            Snapshot kept = testSubject.load(QUALIFIED_NAME, IDENTIFIER, null).orTimeout(5, TimeUnit.SECONDS).join();

            assertThat(kept).isNotNull();
            assertThat(kept.version()).isEqualTo("1.0");
            assertThat(kept.position()).isEqualTo(new GlobalIndexPosition(42));
        }

        @Test
        void store_writesOneSnapshotAtATimeWhenTheProviderCompletesAsynchronously() throws SQLException {
            // given a provider that runs every write on a thread of its own
            ExecutorService threads = Executors.newCachedThreadPool();
            AtomicInteger inFlight = new AtomicInteger();
            AtomicInteger mostInFlight = new AtomicInteger();
            JdbcTransactionalExecutorProvider jdbc = new JdbcTransactionalExecutorProvider(dataSource);
            TransactionalExecutorProvider<Connection> asynchronous = new TransactionalExecutorProvider<>() {
                @Override
                public @Nullable TransactionalExecutor<Connection> findTransactionalExecutor(
                        @Nullable ProcessingContext processingContext
                ) {
                    TransactionalExecutor<Connection> delegate = jdbc.getTransactionalExecutor(processingContext);

                    return new TransactionalExecutor<>() {
                        @Override
                        public <R> CompletableFuture<R> apply(ThrowingFunction<Connection, R, Exception> function) {
                            return CompletableFuture.supplyAsync(() -> {
                                mostInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);

                                try {
                                    Thread.sleep(100);

                                    return delegate.apply(function).join();
                                }
                                catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();

                                    throw new IllegalStateException(e);
                                }
                                finally {
                                    inFlight.decrementAndGet();
                                }
                            }, threads);
                        }
                    };
                }
            };
            PostgresqlSnapshotStore asynchronousSubject =
                    new PostgresqlSnapshotStore(dataSource, new JacksonConverter(), asynchronous);

            try {
                // when
                List<CompletableFuture<Void>> results = new ArrayList<>();

                for (int i = 0; i < 5; i++) {
                    results.add(asynchronousSubject.store(QUALIFIED_NAME, "entity-" + i, snapshot(1, "1.0"), null));
                }

                CompletableFuture.allOf(results.toArray(CompletableFuture[]::new)).orTimeout(30, TimeUnit.SECONDS).join();

                // then
                assertThat(mostInFlight).as("writes in flight at the same time").hasValue(1);
                assertThat(countSnapshots()).isEqualTo(5);
            }
            finally {
                asynchronousSubject.close();
                threads.shutdownNow();
            }
        }

        @Test
        void store_completesTheFutureWhenTheProviderThrowsAnError_andKeepsWriting() throws SQLException {
            // given a provider that fails with an error on its first use
            AtomicBoolean failing = new AtomicBoolean(true);
            JdbcTransactionalExecutorProvider jdbc = new JdbcTransactionalExecutorProvider(dataSource);
            TransactionalExecutorProvider<Connection> failingProvider = new TransactionalExecutorProvider<>() {
                @Override
                public @Nullable TransactionalExecutor<Connection> findTransactionalExecutor(
                        @Nullable ProcessingContext processingContext
                ) {
                    TransactionalExecutor<Connection> delegate = jdbc.getTransactionalExecutor(processingContext);

                    return new TransactionalExecutor<>() {
                        @Override
                        public <R> CompletableFuture<R> apply(ThrowingFunction<Connection, R, Exception> function) {
                            if (failing.get()) {
                                throw new NoClassDefFoundError("simulated");
                            }

                            return delegate.apply(function);
                        }
                    };
                }
            };
            PostgresqlSnapshotStore failingSubject =
                    new PostgresqlSnapshotStore(dataSource, new JacksonConverter(), failingProvider);

            try {
                // when
                CompletableFuture<Void> failed =
                        failingSubject.store(QUALIFIED_NAME, "entity-1", snapshot(1, "1.0"), null);

                // then
                assertThat(failed)
                        .failsWithin(Duration.ofSeconds(10))
                        .withThrowableOfType(ExecutionException.class)
                        .withCauseInstanceOf(NoClassDefFoundError.class);

                // and the next snapshot is still written
                failing.set(false);

                assertThat(failingSubject.store(QUALIFIED_NAME, "entity-2", snapshot(1, "1.0"), null))
                        .succeedsWithin(Duration.ofSeconds(10));
                assertThat(countSnapshots()).isEqualTo(1);
            }
            finally {
                failingSubject.close();
            }
        }
    }

    private static long countSnapshots() throws SQLException {
        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM snapshots")
        ) {
            resultSet.next();

            return resultSet.getLong(1);
        }
    }

    private static Snapshot snapshot(long globalIndex, String version) {
        return new Snapshot(new GlobalIndexPosition(globalIndex), version, "payload".getBytes(), TIMESTAMP, Map.of());
    }
}
