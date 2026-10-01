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
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.junit.jupiter.api.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@code PostgresqlFinalizer}.
 *
 * @author John Hendrikx
 */
class PostgresqlFinalizerTest {

    private static PostgreSQLContainer postgresContainer;
    private static HikariDataSource dataSource;

    private AtomicLong notifiedGlobalIndex;
    private PostgresqlFinalizer testSubject;

    @BeforeAll
    @SuppressWarnings("resource")
    static void startContainer() {
        postgresContainer = new PostgreSQLContainer("postgres:16.2-alpine")
                .withDatabaseName("testdb")
                .withUsername("test")
                .withPassword("test");
        postgresContainer.start();

        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(postgresContainer.getJdbcUrl());
        config.setUsername(postgresContainer.getUsername());
        config.setPassword(postgresContainer.getPassword());
        config.setAutoCommit(true);

        dataSource = new HikariDataSource(config);

        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement()
        ) {
            statement.execute(
                """
                CREATE TABLE events (global_index INT8 PRIMARY KEY);
                CREATE TABLE tags (global_index INT8 NOT NULL, key VARCHAR NOT NULL, value VARCHAR NOT NULL, PRIMARY KEY (key, value, global_index));
                CREATE TABLE consistency_tags (tag_hash INT4 NOT NULL, global_index INT8 NOT NULL, PRIMARY KEY (tag_hash));
                CREATE SEQUENCE events_monotonic_seq INCREMENT BY 1 CACHE 1 OWNED BY events.global_index;
                """
            );
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @AfterAll
    static void stopContainer() {
        dataSource.close();

        if (postgresContainer != null) {
            postgresContainer.stop();
        }
    }

    @BeforeEach
    void setUp() throws SQLException {
        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement()
        ) {
            statement.execute(
                """
                DELETE FROM tags;
                DELETE FROM consistency_tags;
                DELETE FROM events;
                ALTER SEQUENCE events_monotonic_seq RESTART WITH 1;
                """
            );
        }

        notifiedGlobalIndex = new AtomicLong(-1);
        testSubject = new PostgresqlFinalizer(dataSource, notifiedGlobalIndex::set);
    }

    @AfterEach
    void tearDown() {
        testSubject.close();
    }

    @Test
    void scheduleFinalizationWithUnfinalizedEventsAssignsPermanentIndicesAndReturnsMarker() {
        insertEvent(-1);
        insertEvent(-2);

        ConsistencyMarker marker = join(testSubject.scheduleFinalization());

        List<Long> globalIndices = queryAllGlobalIndices();
        assertThat(globalIndices).hasSize(2).allMatch(index -> index > 0);

        long latestGlobalIndex = globalIndices.stream().mapToLong(Long::longValue).max().orElseThrow();
        assertThat(GlobalIndexConsistencyMarker.position(marker)).isEqualTo(latestGlobalIndex + 1);
    }

    @Test
    void scheduleFinalizationWithUnfinalizedEventsNotifiesCallbackWithLatestGlobalIndex() {
        insertEvent(-1);

        ConsistencyMarker marker = join(testSubject.scheduleFinalization());

        assertThat(notifiedGlobalIndex.get()).isEqualTo(GlobalIndexConsistencyMarker.position(marker) - 1);
    }

    @Test
    void scheduleFinalizationWithNoUnfinalizedEventsStillCompletesSuccessfully() {
        ConsistencyMarker marker = join(testSubject.scheduleFinalization());

        assertThat(marker).isInstanceOf(GlobalIndexConsistencyMarker.class);
        assertThat(notifiedGlobalIndex.get()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void scheduleFinalizationAlsoAssignsPermanentIndexToRelatedTagsAndConsistencyTags() {
        insertEvent(-1);
        insertTag(-1, "course", "abc");
        insertConsistencyTag(42, -1);

        join(testSubject.scheduleFinalization());

        long finalizedIndex = queryAllGlobalIndices().getFirst();

        assertThat(queryTagGlobalIndex("course", "abc")).isEqualTo(finalizedIndex);
        assertThat(queryConsistencyTagGlobalIndex(42)).isEqualTo(finalizedIndex);
    }

    @Test
    void concurrentScheduleFinalizationBothCompleteAndFinalizeAllEvents() {
        insertEvent(-1);
        insertEvent(-2);

        CompletableFuture<ConsistencyMarker> first = testSubject.scheduleFinalization();
        CompletableFuture<ConsistencyMarker> second = testSubject.scheduleFinalization();

        assertThat(join(first)).isNotNull();
        assertThat(join(second)).isNotNull();
        assertThat(queryAllGlobalIndices()).hasSize(2).allMatch(index -> index > 0);
    }

    @Test
    void manyConcurrentScheduleFinalizationCallsShareAtMostOneQueuedRun() throws SQLException {
        insertEvent(-1);
        insertEvent(-2);

        // FINALIZE_STATEMENT's first step acquires this same advisory lock, so holding it here
        // guarantees the first scheduled finalization stays in flight for as long as we hold it -
        // deterministic, rather than relying on the real finalization query being "slow enough".
        try (
            Connection lockHolder = dataSource.getConnection();
            Statement lockStatement = lockHolder.createStatement()
        ) {
            lockStatement.execute("SELECT pg_advisory_lock(42)");

            CompletableFuture<ConsistencyMarker> first = testSubject.scheduleFinalization();
            List<CompletableFuture<ConsistencyMarker>> queued = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                queued.add(testSubject.scheduleFinalization());
            }

            // All ten calls made while the first is still blocked on the lock must have been
            // handed the exact same queued future - i.e. exactly one extra run was created for
            // all ten combined, not ten separate ones.
            CompletableFuture<ConsistencyMarker> queuedRun = queued.getFirst();

            assertThat(queued).allMatch(future -> future == queuedRun);

            lockStatement.execute("SELECT pg_advisory_unlock(42)");

            assertThat(join(first)).isNotNull();
            assertThat(join(queuedRun)).isNotNull();
        }

        assertThat(queryAllGlobalIndices()).hasSize(2).allMatch(index -> index > 0);
    }

    @Test
    void twoFinalizerInstancesBothBlockOnAdvisoryLockHeldByAnotherConnection() throws SQLException {
        insertEvent(-1);
        insertEvent(-2);

        PostgresqlFinalizer finalizerA = new PostgresqlFinalizer(dataSource, index -> {});
        PostgresqlFinalizer finalizerB = new PostgresqlFinalizer(dataSource, index -> {});

        try {
            // Simulates another process already holding the same advisory lock key: if
            // FINALIZE_STATEMENT genuinely acquires it, both finalizers below - independent
            // PostgresqlFinalizer instances with their own executors, standing in for two
            // separate engine instances - must wait until this connection releases it.
            try (
                Connection lockHolder = dataSource.getConnection();
                Statement lockStatement = lockHolder.createStatement()
            ) {
                lockStatement.execute("SELECT pg_advisory_lock(42)");

                CompletableFuture<ConsistencyMarker> futureA = finalizerA.scheduleFinalization();
                CompletableFuture<ConsistencyMarker> futureB = finalizerB.scheduleFinalization();

                assertThatThrownBy(() -> futureA.get(300, TimeUnit.MILLISECONDS))
                        .isInstanceOf(TimeoutException.class);
                assertThatThrownBy(() -> futureB.get(300, TimeUnit.MILLISECONDS))
                        .isInstanceOf(TimeoutException.class);

                lockStatement.execute("SELECT pg_advisory_unlock(42)");

                assertThat(join(futureA)).isNotNull();
                assertThat(join(futureB)).isNotNull();
            }

            assertThat(queryAllGlobalIndices()).hasSize(2).allMatch(index -> index > 0);
        }
        finally {
            finalizerA.close();
            finalizerB.close();
        }
    }

    private static ConsistencyMarker join(CompletableFuture<ConsistencyMarker> future) {
        return future.orTimeout(5, TimeUnit.SECONDS).join();
    }

    private void insertEvent(long globalIndex) {
        execute("INSERT INTO events (global_index) VALUES (?)", ps -> ps.setLong(1, globalIndex));
    }

    private void insertTag(long globalIndex, String key, String value) {
        execute("INSERT INTO tags (global_index, key, value) VALUES (?, ?, ?)", ps -> {
            ps.setLong(1, globalIndex);
            ps.setString(2, key);
            ps.setString(3, value);
        });
    }

    private void insertConsistencyTag(int tagHash, long globalIndex) {
        execute("INSERT INTO consistency_tags (tag_hash, global_index) VALUES (?, ?)", ps -> {
            ps.setInt(1, tagHash);
            ps.setLong(2, globalIndex);
        });
    }

    private List<Long> queryAllGlobalIndices() {
        List<Long> result = new ArrayList<>();

        try (
            Connection connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("SELECT global_index FROM events");
            ResultSet resultSet = ps.executeQuery()
        ) {
            while (resultSet.next()) {
                result.add(resultSet.getLong(1));
            }

            return result;
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private long queryTagGlobalIndex(String key, String value) {
        try (
            Connection connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("SELECT global_index FROM tags WHERE key = ? AND value = ?")
        ) {
            ps.setString(1, key);
            ps.setString(2, value);

            try (ResultSet resultSet = ps.executeQuery()) {
                resultSet.next();

                return resultSet.getLong(1);
            }
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private long queryConsistencyTagGlobalIndex(int tagHash) {
        try (
            Connection connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("SELECT global_index FROM consistency_tags WHERE tag_hash = ?")
        ) {
            ps.setInt(1, tagHash);

            try (ResultSet resultSet = ps.executeQuery()) {
                resultSet.next();

                return resultSet.getLong(1);
            }
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @FunctionalInterface
    private interface StatementSetter {
        void apply(PreparedStatement ps) throws SQLException;
    }

    private void execute(String sql, StatementSetter setter) {
        try (
            Connection connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement(sql)
        ) {
            setter.apply(ps);
            ps.execute();
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
