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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Test class validating that appends of the {@link PostgresqlEventStorageEngine} do not deadlock on the
 * {@code consistency_tags} rows they share with each other or with the {@code PostgresqlFinalizer}.
 * <p>
 * Every test keeps one {@code consistency_tags} row, the gate, locked from a separate transaction. An append that
 * needs the gate stops there while it already holds other rows, which makes the order in which the transactions
 * reach their rows the same on every run.
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
            // given - an append that stops at the gate, after its event with the low tag
            CompletableFuture<ConsistencyMarker> first = append(AppendCondition.none(), low, gate, high);
            awaitTransactionsWaitingForALock(1);

            // and - an append that reaches the same tags in the opposite order
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
            // given - an append that stops at the gate, after it locked the high tag of its condition
            AppendCondition condition = AppendCondition.withCriteria(EventCriteria.havingTags(high));
            CompletableFuture<ConsistencyMarker> conditional = append(condition, gate, low);
            awaitTransactionsWaitingForALock(1);

            // and - an append without a condition that reaches the low tag first
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
            // given - committed events with the low and the high tag that still wait for their final global index
            AppendTransaction<Object> committed = appendAndCommit(AppendCondition.none(), low, high);

            // and - an append that stops at the gate, after its event with the high tag
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
    @SuppressWarnings("unchecked")
    private AppendTransaction<Object> appendAndCommit(AppendCondition condition, Tag... tagPerEvent) {
        List<TaggedEventMessage<?>> events = new ArrayList<>();

        for (int i = 0; i < tagPerEvent.length; i++) {
            events.add(new GenericTaggedEventMessage<>(EventTestUtils.createEvent(i), Set.of(tagPerEvent[i])));
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
                        "SELECT COUNT(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'"
                )
            ) {
                resultSet.next();

                return resultSet.getInt(1) == expected;
            }
        });
    }

    private int tagHash(Tag tag) {
        return MurmurHash3.hash32((tag.key() + ":" + tag.value()).getBytes(StandardCharsets.UTF_8))
                & testSubject.hashMask;
    }
}
