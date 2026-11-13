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
import org.axonframework.common.jdbc.ConnectionExecutor;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine.AppendTransaction;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Test class validating that appends of the {@link PostgresqlEventStorageEngine} do not deadlock on the
 * {@code consistency_tags} rows they share with each other or with the {@code PostgresqlFinalizer}, and that the
 * rows they store stay correct.
 * <p>
 * A separate transaction keeps one {@code consistency_tags} row, the gate, locked. An append that needs the gate
 * stops there while it already holds other rows, which makes the order in which the transactions reach their rows
 * the same on every run.
 *
 * @author Stefan Dragisic
 */
class PostgresqlEventStorageEngineAppendLockOrderTest {

    private static final EventConverter CONVERTER = new DelegatingEventConverter(new JacksonConverter());
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private static PostgreSQLContainer postgresContainer;
    private static HikariDataSource dataSource;

    private PostgresqlEventStorageEngine testSubject;
    private ExecutorService appendThreads;
    private Connection gateHolder;

    /**
     * Three tags in ascending {@code tag_hash} order.
     */
    private Tag low;
    private Tag high;
    private Tag gate;

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
        config.setAutoCommit(false);

        dataSource = new HikariDataSource(config);
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
        testSubject = new PostgresqlEventStorageEngine(dataSource, CONVERTER);
        appendThreads = Executors.newFixedThreadPool(2);

        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement()
        ) {
            statement.execute("TRUNCATE events, tags, consistency_tags");
            connection.commit();
        }

        List<Tag> tagsByHash = Stream.of("a", "b", "c")
                                     .map(value -> new Tag("lockOrder", value))
                                     .sorted(Comparator.comparingInt(this::tagHash))
                                     .toList();

        low = tagsByHash.get(0);
        high = tagsByHash.get(1);
        gate = tagsByHash.get(2);

        assertThat(tagHash(low))
                .as("fixture assumption: the three tags must have distinct hashes")
                .isLessThan(tagHash(high));
        assertThat(tagHash(high)).isLessThan(tagHash(gate));

        gateHolder = dataSource.getConnection();

        try (PreparedStatement ps = gateHolder.prepareStatement(
                "INSERT INTO consistency_tags (tag_hash, global_index) VALUES (?, 0)"
        )) {
            ps.setInt(1, tagHash(gate));
            ps.execute();
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        gateHolder.close();
        appendThreads.shutdownNow();
        testSubject.close();
    }

    @Nested
    class ConcurrentAppends {

        @Test
        void appendsTaggingTheirEventsInOppositeOrderBothSucceed() throws SQLException {
            // given
            // an append that stops at the gate, after its event with the low tag
            CompletableFuture<ConsistencyMarker> first = append(AppendCondition.none(), low, gate, high);
            awaitTransactionsWaitingForALock(1);
            // an append that reaches the same tags in the opposite order
            CompletableFuture<ConsistencyMarker> second = append(AppendCondition.none(), high, gate, low);
            awaitTransactionsWaitingForALock(2);

            // when
            gateHolder.rollback();

            // then
            assertThat(first).succeedsWithin(TIMEOUT);
            assertThat(second).succeedsWithin(TIMEOUT);
        }

        @Test
        void appendWithConditionAndAppendWithoutReachingTheSameTagsInOppositeOrderBothSucceed() throws SQLException {
            // given
            // an append that stops at the gate, after it locked the high tag of its condition
            AppendCondition condition = AppendCondition.withCriteria(EventCriteria.havingTags(high));
            CompletableFuture<ConsistencyMarker> conditional = append(condition, gate, low);
            awaitTransactionsWaitingForALock(1);
            // an append without a condition that reaches the low tag first
            CompletableFuture<ConsistencyMarker> unconditional = append(AppendCondition.none(), low, gate, high);
            awaitTransactionsWaitingForALock(2);

            // when
            gateHolder.rollback();

            // then
            assertThat(conditional).succeedsWithin(TIMEOUT);
            assertThat(unconditional).succeedsWithin(TIMEOUT);
        }
    }

    @Nested
    class AppendDuringFinalization {

        @Test
        void appendSharingTagsWithEventsBeingFinalizedSucceedsAndSoDoesTheFinalization() throws SQLException {
            // given
            // committed events with the low and the high tag that still wait for their final global index
            AppendTransaction<Object> committed = appendAndCommit(AppendCondition.none(), low, high);
            // an append that stops at the gate, after its event with the high tag
            CompletableFuture<ConsistencyMarker> append = append(AppendCondition.none(), high, gate, low);
            awaitTransactionsWaitingForALock(1);

            // when - the committed events are finalized while the append is in flight
            CompletableFuture<ConsistencyMarker> finalization = committed.afterCommit(null);
            awaitTransactionsWaitingForALock(2);
            gateHolder.rollback();

            // then
            assertThat(finalization).succeedsWithin(TIMEOUT);
            assertThat(append).succeedsWithin(TIMEOUT);
        }
    }

    @Nested
    class StoredConsistencyTags {

        private final Tag x = new Tag("stored", "x");
        private final Tag y = new Tag("stored", "y");
        private final Tag z = new Tag("stored", "z");

        @Test
        void tagOfALaterEventEndsAtTheIndexOfThatEvent() {
            // when
            finalize(appendAndCommit(AppendCondition.none(), List.of(Set.of(x), Set.of(y), Set.of(x))));

            // then
            assertThat(consistencyTagIndices()).isEqualTo(latestEventIndexPerTag()).hasSize(2);
        }

        @Test
        void eventsWithSeveralTagsAndTagsRepeatedAcrossEventsEndAtTheIndexOfTheLatestEventPerTag() {
            // when
            finalize(appendAndCommit(AppendCondition.none(), List.of(Set.of(x, y), Set.of(y, z), Set.of(x))));

            // then
            assertThat(consistencyTagIndices()).isEqualTo(latestEventIndexPerTag()).hasSize(3);
            assertThat(eventCount()).isEqualTo(3);
        }

        @Test
        void conditionalAppendWithTagsOutsideItsConditionEndsAtTheIndexOfTheLatestEventPerTag() {
            // given
            AppendCondition condition = AppendCondition.withCriteria(EventCriteria.havingTags(x));

            // when
            finalize(appendAndCommit(condition, List.of(Set.of(x, y), Set.of(z), Set.of(x, z))));

            // then
            assertThat(consistencyTagIndices()).isEqualTo(latestEventIndexPerTag()).hasSize(3);
        }

        @Test
        void conflictingAppendWithTagsOutsideItsConditionIsRejectedAndStoresNothing() {
            // given
            finalize(appendAndCommit(AppendCondition.none(), List.of(Set.of(x))));
            Map<Integer, Long> storedBefore = consistencyTagIndices();
            AppendCondition conflicting = AppendCondition.withCriteria(EventCriteria.havingTags(x));

            // when / then
            assertThatThrownBy(() -> appendAndCommit(conflicting, List.of(Set.of(x, y), Set.of(z))))
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(AppendEventsTransactionRejectedException.class);
            assertThat(eventCount()).isEqualTo(1);
            assertThat(consistencyTagIndices()).isEqualTo(storedBefore);
        }

        @Test
        void appendWithoutTagsOrConditionStoresItsEventsAndNoConsistencyTags() {
            // when
            finalize(appendAndCommit(AppendCondition.none(), List.of(Set.of(), Set.of())));

            // then
            assertThat(eventCount()).isEqualTo(2);
            assertThat(consistencyTagIndices()).isEmpty();
        }

        private void finalize(AppendTransaction<Object> transaction) {
            assertThat(transaction.afterCommit(null)).succeedsWithin(TIMEOUT);
        }
    }

    /**
     * Appends one event per given tag, in the given order, on its own thread and in its own transaction, commits,
     * and finalizes.
     */
    private CompletableFuture<ConsistencyMarker> append(AppendCondition condition, Tag... tagPerEvent) {
        return CompletableFuture.supplyAsync(() -> appendAndCommit(condition, tagPerEvent), appendThreads)
                                .thenCompose(transaction -> transaction.afterCommit(null));
    }

    /**
     * Appends one event per given tag, in the given order, and commits. The events keep their temporary global
     * index until {@link AppendTransaction#afterCommit(Object)} of the returned transaction is called.
     */
    private AppendTransaction<Object> appendAndCommit(AppendCondition condition, Tag... tagPerEvent) {
        return appendAndCommit(condition, Arrays.stream(tagPerEvent).map(Set::of).toList());
    }

    /**
     * Appends one event per given set of tags, in the given order, and commits. See
     * {@link #appendAndCommit(AppendCondition, Tag...)}.
     */
    @SuppressWarnings("unchecked")
    private AppendTransaction<Object> appendAndCommit(AppendCondition condition, List<Set<Tag>> tagsPerEvent) {
        List<TaggedEventMessage<?>> events = new ArrayList<>();

        for (int i = 0; i < tagsPerEvent.size(); i++) {
            events.add(new GenericTaggedEventMessage<>(EventTestUtils.createEvent(i), tagsPerEvent.get(i)));
        }

        try (Connection connection = dataSource.getConnection()) {
            StubProcessingContext context = new StubProcessingContext();

            context.putResource(
                    JdbcTransactionalExecutorProvider.SUPPLIER_KEY,
                    () -> new ConnectionExecutor(() -> connection)
            );

            try {
                AppendTransaction<Object> transaction = (AppendTransaction<Object>)
                        testSubject.appendEvents(condition, context, events).orTimeout(30, TimeUnit.SECONDS).join();

                transaction.commit().orTimeout(30, TimeUnit.SECONDS).join();
                connection.commit();

                return transaction;
            }
            catch (RuntimeException e) {
                connection.rollback();

                throw e;
            }
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private void awaitTransactionsWaitingForALock(int expected) {
        await().atMost(TIMEOUT).until(() -> {
            try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(
                        "SELECT COUNT(*) FROM pg_stat_activity "
                        + "WHERE wait_event_type = 'Lock' AND datname = current_database()"
                )
            ) {
                resultSet.next();

                return resultSet.getInt(1) == expected;
            }
        });
    }

    /**
     * Returns the {@code consistency_tags} hash of the given tag. Replicates the engine's hash on purpose: stored
     * hashes must never change, so a change to the engine's hash has to break this test.
     */
    private int tagHash(Tag tag) {
        return MurmurHash3.hash32((tag.key() + ":" + tag.value()).getBytes(StandardCharsets.UTF_8))
                & testSubject.hashMask;
    }

    /**
     * Returns the committed {@code global_index} of every {@code consistency_tags} row, by {@code tag_hash}.
     */
    private Map<Integer, Long> consistencyTagIndices() {
        Map<Integer, Long> indices = new HashMap<>();

        query("SELECT tag_hash, global_index FROM consistency_tags",
              resultSet -> indices.put(resultSet.getInt(1), resultSet.getLong(2)));

        return indices;
    }

    /**
     * Returns the final {@code global_index} of the latest event carrying each tag, by the tag's {@code tag_hash}.
     */
    private Map<Integer, Long> latestEventIndexPerTag() {
        Map<Integer, Long> indices = new HashMap<>();

        query("SELECT key, value, MAX(global_index) FROM tags WHERE key <> '__T' GROUP BY key, value",
              resultSet -> {
                  assertThat(resultSet.getLong(3)).as("event is finalized").isPositive();
                  indices.put(tagHash(new Tag(resultSet.getString(1), resultSet.getString(2))), resultSet.getLong(3));
              });

        return indices;
    }

    private long eventCount() {
        List<Long> count = new ArrayList<>();

        query("SELECT COUNT(*) FROM events", resultSet -> count.add(resultSet.getLong(1)));

        return count.getFirst();
    }

    private void query(String sql, RowConsumer rowConsumer) {
        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql)
        ) {
            while (resultSet.next()) {
                rowConsumer.accept(resultSet);
            }
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @FunctionalInterface
    private interface RowConsumer {

        void accept(ResultSet resultSet) throws SQLException;
    }
}
