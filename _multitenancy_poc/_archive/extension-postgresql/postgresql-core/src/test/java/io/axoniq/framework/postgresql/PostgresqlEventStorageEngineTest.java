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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.axonframework.conversion.json.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine.AppendTransaction;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.StorageEngineTestSuite;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageStream.Entry;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Test class validating the {@link PostgresqlEventStorageEngine}.
 *
 * @author John Hendrikx
 */
class PostgresqlEventStorageEngineTest extends StorageEngineTestSuite<PostgresqlEventStorageEngine> {
    private static final EventConverter CONVERTER = new DelegatingEventConverter(new JacksonConverter());

    private static PostgreSQLContainer<?> postgresContainer;
    private static DataSource dataSource;

    @AfterAll
    static void stopContainer() {
        if (postgresContainer != null) {
            postgresContainer.stop();
        }
    }

    private final ConnectionExecutor connectionExecutor = new ConnectionExecutor() {
        @Override
        public <T> T execute(ProcessingContext context, JdbcFunction<Connection, T> function) {
            try (Connection connection = dataSource.getConnection()) {
                T result = function.apply(connection);

                connection.commit();

                return result;
            }
            catch (SQLException e) {
                throw new IllegalStateException("SQL callback failed with an SQLException", e);
            }
        }
    };

    @Override
    @SuppressWarnings("resource")
    protected PostgresqlEventStorageEngine buildStorageEngine() throws SQLException {
        postgresContainer = new PostgreSQLContainer<>("postgres:16.2")
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

        return new PostgresqlEventStorageEngine(connectionExecutor, CONVERTER);
    }

    @Override
    protected ProcessingContext processingContext() {
        return null;
    }

    @Test
    void streamShouldBeNotifiedOfAppend() {
        TrackingToken latest = testSubject.latestToken(null).join();

        // Create a stream to see what if it is notified of a new event:
        MessageStream<EventMessage> stream = testSubject.stream(StreamingCondition.startingFrom(latest), null);

        finishTx(testSubject.appendEvents(AppendCondition.none(), null, List.of(
            taggedEventMessage("Hello World", Set.of())
        )));

        // Assert that the event has become available:
        await().untilAsserted(() -> assertThat(stream.hasNextAvailable()).isTrue());

        assertThat(stream.next())
            .map(Entry::message)
            .map(em -> em.payloadAs(String.class, CONVERTER))
            .contains("Hello World");
    }

    @Test
    void twoIndependentStorageEnginesShouldSeeEachOthersAppends() {
        EventStorageEngine engine1 = testSubject;
        EventStorageEngine engine2 = new PostgresqlEventStorageEngine(connectionExecutor, CONVERTER);

        TrackingToken latest = engine1.latestToken(null).join();

        assertThat(latest).isEqualTo(engine2.latestToken(null).join());

        // Create a stream on both engines, to see what events are being appended:
        MessageStream<EventMessage> stream1 = engine1.stream(StreamingCondition.startingFrom(latest), null);
        MessageStream<EventMessage> stream2 = engine1.stream(StreamingCondition.startingFrom(latest), null);

        // Append an event via engine 1:
        finishTx(engine1.appendEvents(AppendCondition.none(), null, List.of(
            taggedEventMessage("Hello From Engine 1", Set.of())
        )));

        // Assert that both engines see the event:
        await().untilAsserted(() -> {
            assertThat(stream1.hasNextAvailable()).isTrue();
            assertThat(stream2.hasNextAvailable()).isTrue();
        });

        assertThat(stream1.next())
            .map(Entry::message)
            .map(em -> em.payloadAs(String.class, CONVERTER))
            .contains("Hello From Engine 1");

        assertThat(stream2.next())
            .map(Entry::message)
            .map(em -> em.payloadAs(String.class, CONVERTER))
            .contains("Hello From Engine 1");

        // Append an event via engine 2:
        finishTx(engine1.appendEvents(AppendCondition.none(), null, List.of(
            taggedEventMessage("Hello From Engine 2", Set.of())
        )));

        // Assert that both engines see the event:
        await().untilAsserted(() -> {
            assertThat(stream1.hasNextAvailable()).isTrue();
            assertThat(stream2.hasNextAvailable()).isTrue();
        });

        assertThat(stream1.next())
            .map(Entry::message)
            .map(em -> em.payloadAs(String.class, CONVERTER))
            .contains("Hello From Engine 2");

        assertThat(stream2.next())
            .map(Entry::message)
            .map(em -> em.payloadAs(String.class, CONVERTER))
            .contains("Hello From Engine 2");
    }

    private CompletableFuture<ConsistencyMarker> finishTx(CompletableFuture<AppendTransaction<?>> future) {
        return future
            .thenApply(this::castTransaction)
            .thenCompose(tx -> tx.commit(processingContext())
                 .thenCompose(r -> tx.afterCommit(r, processingContext()))
            );
    }

    private static TaggedEventMessage<EventMessage> taggedEventMessage(String payload, Set<Tag> tags) {
        return new GenericTaggedEventMessage<>(
            new GenericEventMessage(
                UUID.randomUUID().toString(),
                new MessageType("event"),
                payload,
                Map.of("key", "value"),
                Instant.now()
            ),
            tags
        );
    }
}
