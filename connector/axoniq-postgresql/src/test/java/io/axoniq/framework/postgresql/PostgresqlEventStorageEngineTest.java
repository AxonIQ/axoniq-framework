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
import io.axoniq.license.entitlement.EnforcingEntitlementManager;
import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.jdbc.ConnectionExecutor;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.StorageEngineTestSuite;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;
import org.mockito.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
            postgresContainer = new PostgreSQLContainer("postgres:16.2")
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
}
