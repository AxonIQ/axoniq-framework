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
import io.axoniq.license.entitlement.EnforcingEntitlementManager;
import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.jdbc.ConnectionExecutor;
import org.axonframework.conversion.CachingSupplier;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine.AppendTransaction;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.StorageEngineTestSuite;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;
import org.mockito.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link PostgresqlEventStorageEngine}.
 *
 * @author John Hendrikx
 */
class PostgresqlEventStorageEngineTest extends StorageEngineTestSuite<PostgresqlEventStorageEngine> {

    private static final EventConverter CONVERTER = new DelegatingEventConverter(new JacksonConverter());
    private static final ResourceKey<Connection> CONNECTION = ResourceKey.withLabel("connection");

    private PostgreSQLContainer postgresContainer;
    private DataSource dataSource;
    private EntitlementManager entitlementManager;
    private final List<HikariDataSource> createdDataSources = new ArrayList<>();

    @Override
    @SuppressWarnings("resource")
    protected PostgresqlEventStorageEngine createStorageEngine() throws SQLException {
        if (postgresContainer == null) {
            entitlementManager = Mockito.mock(EnforcingEntitlementManager.class);
            postgresContainer = new PostgreSQLContainer("postgres:16.2-alpine")
                    .withDatabaseName("testdb")
                    .withUsername("test")
                    .withPassword("test");

            postgresContainer.start();
        }

        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(postgresContainer.getJdbcUrl());
        config.setUsername(postgresContainer.getUsername());
        config.setPassword(postgresContainer.getPassword());
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(1);
        config.setAutoCommit(false);

        HikariDataSource ds = new HikariDataSource(config);

        createdDataSources.add(ds);
        dataSource = ds;

        return new PostgresqlEventStorageEngine(
                dataSource, CONVERTER, SchemaInitialization.CREATE_IF_MISSING, entitlementManager
        );
    }

    @Override
    protected void disposeStorageEngine(PostgresqlEventStorageEngine engine) throws Exception {
        // Blocks until the monitoring thread has fully stopped, so the datasource can be
        // closed cleanly before the container is terminated in tearDownSuite().
        engine.close();
    }

    @Override
    protected void tearDownSuite() throws Exception {
        createdDataSources.forEach(HikariDataSource::close);
        if (postgresContainer != null) {
            postgresContainer.stop();
        }
    }

    @Override
    protected void commit(ProcessingContext pc) {
        Connection connection = pc.removeResource(CONNECTION);

        if (connection != null) {
            try {
                connection.commit();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    @Override
    protected void rollback(ProcessingContext pc) {
        Connection connection = pc.removeResource(CONNECTION);

        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    @Override
    protected ProcessingContext processingContext() {
        try {
            StubProcessingContext context = new StubProcessingContext();
            Connection connection = dataSource.getConnection();

            connection.setAutoCommit(false);

            context.putResource(CONNECTION, connection);

            context.putResource(
                    JdbcTransactionalExecutorProvider.SUPPLIER_KEY,
                    () -> new ConnectionExecutor(() -> connection)
            );

            return context;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void appendingEventsFailsWhenEntitlementManagerDeniesAccess() {
        try {
            // Configure mock to throw exception when claimMessage is called
            Mockito.doThrow(new RuntimeException("Entitlement denied"))
                   .when(entitlementManager)
                   .claimMessage(Mockito.any(), Mockito.any(), Mockito.anyInt());

            // Attempt to append an event should fail
            TaggedEventMessage<EventMessage> event = taggedEventMessage("event-0", TEST_CRITERIA_TAGS);

            assertThatThrownBy(() -> appendEvents(AppendCondition.none(), event))
                    .isInstanceOf(AssertionError.class)
                    .cause()
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("Entitlement denied");

            // Verify no events were stored
            SourcingCondition condition = SourcingCondition.conditionFor(TEST_CRITERIA);
            MessageStream<EventMessage> stream = testSubject.source(condition);

            waitUntilHasNextAvailable(stream);

            // Should only have the consistency marker, no actual events
            Optional<MessageStream.Entry<EventMessage>> entry = stream.next();
            assertThat(entry).isPresent();
            assertMarkerEntry(entry.get());

            // No more entries
            assertThat(stream.hasNextAvailable()).isFalse();
        } finally {
            // Reset mock to avoid affecting other tests
            Mockito.reset(entitlementManager);
        }
    }

    @Test
    void appendingEventsWithReservedTypeTagKeyThrows() {
        TaggedEventMessage<EventMessage> event = taggedEventMessage("event-0", Set.of(new Tag("__T", "bogus")));

        assertThatThrownBy(() -> appendEvents(AppendCondition.none(), event))
                .isInstanceOf(AssertionError.class)
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("__T");
    }

    @Test
    void sourcingAfterAppendWithinTheSameProcessingContextReusesTheAmbientConnection() throws Exception {
        // given - a context whose connection executor is memoized (like a real transaction manager would
        // register it), tracking how many times a connection was actually handed out
        AtomicInteger connectionsHandedOut = new AtomicInteger();
        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(false);

        StubProcessingContext pc = new StubProcessingContext();
        pc.putResource(JdbcTransactionalExecutorProvider.SUPPLIER_KEY, CachingSupplier.of(() -> new ConnectionExecutor(() -> {
            connectionsHandedOut.incrementAndGet();
            return connection;
        })));

        try {
            // when - append materializes the ambient connection, then source reads afterwards on the same context
            AppendTransaction<?> tx = testSubject.appendEvents(
                    AppendCondition.none(), pc, List.of(taggedEventMessage("event-0", TEST_CRITERIA_TAGS))
            ).get(5, TimeUnit.SECONDS);
            tx.commit().get(5, TimeUnit.SECONDS);
            connection.commit();

            MessageStream<EventMessage> stream = testSubject.source(SourcingCondition.conditionFor(TEST_CRITERIA), pc);
            waitUntilHasNextAvailable(stream);

            // then - the read reused the already-active connection rather than opening a second one
            assertThat(connectionsHandedOut).hasValue(1);
        }
        finally {
            connection.close();
        }
    }

    @Nested
    class SourcingAcrossPages {

        private static final String TAG_ROWS_READ =
                """
                SELECT pg_stat_get_xact_tuples_returned('tags'::regclass)
                     + COALESCE(SUM(pg_stat_get_xact_tuples_returned(indexrelid)), 0)
                  FROM pg_index
                  WHERE indrelid = 'tags'::regclass
                """;

        @Test
        void readsEachTagRowAboutOnceForAnEntityLongerThanAPage() throws Exception {
            // given - an entity with many pages of events, sourced on a connection this test holds open
            int eventCount = 1_000;
            appendInBatches(IntStream.range(0, eventCount)
                                     .mapToObj(i -> taggedEventMessage("event-" + i, TEST_CRITERIA_TAGS))
                                     .toList());

            try (Connection connection = dataSource.getConnection()) {
                connection.setAutoCommit(false);
                long tagRowsReadBefore = tagRowsRead(connection);

                // when
                List<String> payloads = payloadsOf(testSubject.source(
                        SourcingCondition.conditionFor(TEST_CRITERIA), contextReading(connection)
                ));

                // then - a paged read touches each tag row about once. The bound relies on the engine's page
                // size of 50: 1,000 events are 20 pages, and collecting every remaining tag row for each of
                // them reads about ten times the number of events
                long tagRowsRead = tagRowsRead(connection) - tagRowsReadBefore;

                assertThat(payloads).hasSize(eventCount);
                assertThat(tagRowsRead).isLessThanOrEqualTo(2L * eventCount);
            }
        }

        @Test
        void returnsTheEventsOfEitherCriterionInOrder() {
            // given - two entities with interleaved events, each longer than a page, and an unrelated third
            Set<Tag> unrelatedTags = Set.of(new Tag("UNRELATED", TEST_DOMAIN_ID));
            int eventCount = 360;
            appendInBatches(IntStream.range(0, eventCount).mapToObj(i -> taggedEventMessage(
                    "event-" + i, i % 3 == 0 ? TEST_CRITERIA_TAGS : i % 3 == 1 ? OTHER_CRITERIA_TAGS : unrelatedTags
            )).toList());

            // when
            List<String> payloads = payloadsOf(testSubject.source(
                    SourcingCondition.conditionFor(TEST_CRITERIA.or(OTHER_CRITERIA))
            ));

            // then
            assertThat(payloads).containsExactlyElementsOf(
                    IntStream.range(0, eventCount).filter(i -> i % 3 != 2).mapToObj(i -> "event-" + i).toList()
            );
        }

        @Test
        void returnsOnlyTheEventsMatchingEveryTagAndTheTypeInOrder() {
            // given - events that match both tags and the type, interleaved with events that miss one of the three
            Tag firstTag = new Tag("TEST", TEST_DOMAIN_ID);
            Tag secondTag = new Tag("OTHER", OTHER_DOMAIN_ID);
            Set<Tag> bothTags = Set.of(firstTag, secondTag);
            int eventCount = 480;
            appendInBatches(IntStream.range(0, eventCount).mapToObj(i -> switch (i % 4) {
                case 0 -> taggedEventMessage("event-" + i, bothTags);
                case 1 -> taggedEventMessage("event-" + i, Set.of(firstTag));
                case 2 -> taggedEventMessage("event-" + i, Set.of(secondTag));
                default -> typedEvent("other-event", "event-" + i, bothTags);
            }).toList());

            // when
            List<String> payloads = payloadsOf(testSubject.source(SourcingCondition.conditionFor(
                    EventCriteria.havingTags(firstTag, secondTag).andBeingOneOfTypes("event")
            )));

            // then
            assertThat(payloads).containsExactlyElementsOf(
                    IntStream.range(0, eventCount).filter(i -> i % 4 == 0).mapToObj(i -> "event-" + i).toList()
            );
        }

        @Test
        void returnsTheEventsOfAGroupedAndAnUngroupedCriterionInOrder() {
            // given - a criterion with a tag and a type, and a criterion with a single tag; each matches more
            // than a page of events, interleaved with events that have the tag of the first but not its type
            int eventCount = 360;
            appendInBatches(IntStream.range(0, eventCount).mapToObj(i -> switch (i % 3) {
                case 0 -> taggedEventMessage("event-" + i, TEST_CRITERIA_TAGS);
                case 1 -> taggedEventMessage("event-" + i, OTHER_CRITERIA_TAGS);
                default -> typedEvent("other-event", "event-" + i, TEST_CRITERIA_TAGS);
            }).toList());
            EventCriteria tagAndType = EventCriteria.havingTags(TEST_CRITERIA_TAGS).andBeingOneOfTypes("event");

            // when
            List<String> payloads = payloadsOf(testSubject.source(
                    SourcingCondition.conditionFor(tagAndType.or(OTHER_CRITERIA))
            ));

            // then
            assertThat(payloads).containsExactlyElementsOf(
                    IntStream.range(0, eventCount).filter(i -> i % 3 != 2).mapToObj(i -> "event-" + i).toList()
            );
        }

        @Test
        void returnsTheEventsOfSeveralTypesInOrder() {
            // given - events of three types, interleaved. The type names are unique to this test, because a
            // criterion on types alone matches the events of every entity in the store
            String firstType = "first-" + TEST_DOMAIN_ID;
            String secondType = "second-" + TEST_DOMAIN_ID;
            String thirdType = "third-" + TEST_DOMAIN_ID;
            int eventCount = 360;
            appendInBatches(IntStream.range(0, eventCount).mapToObj(i -> typedEvent(
                    i % 3 == 0 ? firstType : i % 3 == 1 ? secondType : thirdType, "event-" + i, TEST_CRITERIA_TAGS
            )).toList());

            // when
            List<String> payloads = payloadsOf(testSubject.source(SourcingCondition.conditionFor(
                    EventCriteria.havingAnyTag().andBeingOneOfTypes(firstType, secondType)
            )));

            // then
            assertThat(payloads).containsExactlyElementsOf(
                    IntStream.range(0, eventCount).filter(i -> i % 3 != 2).mapToObj(i -> "event-" + i).toList()
            );
        }

        private TaggedEventMessage<EventMessage> typedEvent(String type, String payload, Set<Tag> tags) {
            return new GenericTaggedEventMessage<>(new GenericEventMessage(new MessageType(type), payload), tags);
        }

        private void appendInBatches(List<? extends TaggedEventMessage<?>> events) {
            int batchSize = 250;

            for (int from = 0; from < events.size(); from += batchSize) {
                appendEvents(
                        AppendCondition.none(),
                        events.subList(from, Math.min(from + batchSize, events.size()))
                              .toArray(TaggedEventMessage[]::new)
                );
            }
        }

        private ProcessingContext contextReading(Connection connection) throws Exception {
            ConnectionExecutor executor = new ConnectionExecutor(() -> connection);

            // activate the connection, so the engine reads on it
            executor.apply(active -> active).get(5, TimeUnit.SECONDS);

            StubProcessingContext context = new StubProcessingContext();

            context.putResource(JdbcTransactionalExecutorProvider.SUPPLIER_KEY, () -> executor);

            return context;
        }

        private long tagRowsRead(Connection connection) throws SQLException {
            try (
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(TAG_ROWS_READ)
            ) {
                resultSet.next();

                return resultSet.getLong(1);
            }
        }

        private List<String> payloadsOf(MessageStream<EventMessage> stream) {
            try {
                return stream.filter(entry -> entry.message() != TerminalEventMessage.INSTANCE)
                             .collect(
                                     ArrayList<String>::new,
                                     (payloads, event) -> payloads.add(event.payloadAs(String.class))
                             )
                             .get(30, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                throw new AssertionError(e);
            }
        }
    }
}
