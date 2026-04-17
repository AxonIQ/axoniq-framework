/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.framework.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.axonframework.common.jdbc.ConnectionExecutor;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.StorageEngineTestSuite;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Test class validating the {@link PostgresqlEventStorageEngine}.
 *
 * @author John Hendrikx
 */
@Tag("flaky") // TODO: had to mark this flaky because it broke the local build continuously.
class PostgresqlEventStorageEngineTest extends StorageEngineTestSuite<PostgresqlEventStorageEngine> {

    private static final EventConverter CONVERTER = new DelegatingEventConverter(new JacksonConverter());
    private static final ResourceKey<Connection> CONNECTION = ResourceKey.withLabel("connection");

    private static PostgreSQLContainer postgresContainer;
    private static DataSource dataSource;

    @Override
    @SuppressWarnings("resource")
    protected PostgresqlEventStorageEngine createStorageEngine() throws SQLException {
        if (postgresContainer == null) {
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

        dataSource = new HikariDataSource(config);

        return new PostgresqlEventStorageEngine(dataSource, CONVERTER);
    }

    @Override
    protected void disposeStorageEngine(PostgresqlEventStorageEngine engine) throws Exception {
        engine.close();
    }

    @Override
    protected void tearDownSuite() throws Exception {
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
}
