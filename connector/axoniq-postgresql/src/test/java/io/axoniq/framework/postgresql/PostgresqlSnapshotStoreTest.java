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
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AggregateSequenceNumberPosition;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.api.SnapshotCandidate;
import org.axonframework.eventsourcing.snapshot.api.SnapshotMatch;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.junit.jupiter.api.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    class LoadWithCandidates {

        private static final QualifiedName FORMER_NAME = new QualifiedName("com.example.FormerAggregate");

        @Test
        void shouldReturnTheSnapshotOfTheFirstCandidateWithAnAcceptableOne() {
            // given
            store(QUALIFIED_NAME, snapshot(42, "1.0"));
            store(FORMER_NAME, snapshot(43, "1.0"));

            // when
            SnapshotMatch match = load(candidate(QUALIFIED_NAME, "1.0"), candidate(FORMER_NAME, "1.0"));

            // then
            assertThat(match).isNotNull();
            assertThat(match.qualifiedName()).isEqualTo(QUALIFIED_NAME);
            assertThat(match.snapshot().position()).isEqualTo(new GlobalIndexPosition(42));
        }

        @Test
        void shouldFallBackToALaterCandidateWhenAnEarlierOneHasNoSnapshot() {
            // given
            store(FORMER_NAME, snapshot(43, "1.0"));

            // when
            SnapshotMatch match = load(candidate(QUALIFIED_NAME, "1.0"), candidate(FORMER_NAME, "1.0"));

            // then
            assertThat(match).isNotNull();
            assertThat(match.qualifiedName()).isEqualTo(FORMER_NAME);
        }

        @Test
        void shouldPassOverASnapshotOfAVersionThatIsNotAcceptable() {
            // given a snapshot written by a newer version of the application than the one now loading it
            store(QUALIFIED_NAME, snapshot(42, "2.0"));
            store(FORMER_NAME, snapshot(43, "1.0"));

            // when
            SnapshotMatch match = load(candidate(QUALIFIED_NAME, "1.0"), candidate(FORMER_NAME, "1.0"));

            // then
            assertThat(match).isNotNull();
            assertThat(match.qualifiedName()).isEqualTo(FORMER_NAME);
        }

        @Test
        void shouldAcceptAnyVersionInTheAcceptableSet() {
            // given
            store(QUALIFIED_NAME, snapshot(42, "2.0"));

            // when
            SnapshotMatch match = load(new SnapshotCandidate(QUALIFIED_NAME, Set.of("1.0", "2.0")));

            // then
            assertThat(match).isNotNull();
            assertThat(match.snapshot().version()).isEqualTo("2.0");
        }

        @Test
        void shouldReturnNullWhenNoCandidateYieldsAnAcceptableSnapshot() {
            // given
            store(QUALIFIED_NAME, snapshot(42, "2.0"));

            // when
            SnapshotMatch match = load(candidate(QUALIFIED_NAME, "1.0"));

            // then
            assertThat(match).isNull();
        }

        // temporary: covers the any-version candidate Axon Framework allows only while its deprecated bridge exists
        @Test
        void shouldAcceptAnyVersionForACandidateWithNoVersions() {
            // given
            store(QUALIFIED_NAME, snapshot(42, "2.0"));

            // when
            SnapshotMatch match = load(new SnapshotCandidate(QUALIFIED_NAME, Set.of()));

            // then
            assertThat(match).isNotNull();
            assertThat(match.snapshot().version()).isEqualTo("2.0");
        }

        private static void store(QualifiedName name, Snapshot snapshot) {
            testSubject.store(name, IDENTIFIER, snapshot, null).orTimeout(5, TimeUnit.SECONDS).join();
        }

        private static SnapshotMatch load(SnapshotCandidate... candidates) {
            return testSubject.load(List.of(candidates), IDENTIFIER, null).orTimeout(5, TimeUnit.SECONDS).join();
        }

        private static SnapshotCandidate candidate(QualifiedName name, String version) {
            return new SnapshotCandidate(name, Set.of(version));
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

            // when / then -- must throw synchronously, before any async work
            assertThatThrownBy(() -> testSubject.store(QUALIFIED_NAME, IDENTIFIER, snapshot, null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unsupported position type");
        }
    }

    private static Snapshot snapshot(long globalIndex, String version) {
        return new Snapshot(new GlobalIndexPosition(globalIndex), version, "payload".getBytes(), TIMESTAMP, Map.of());
    }
}
