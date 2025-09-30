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
import org.axonframework.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.eventsourcing.eventstore.SimpleEventStore;
import org.axonframework.messaging.unitofwork.ProcessingContext;
import org.axonframework.serialization.json.JacksonConverter;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

/**
 * Test class validating the {@link SimpleEventStore} together with the {@link PostgresqlEventStorageEngine}.
 *
 * @author John Hendrikx
 */
class PostgresqlEventStorageEngineTest extends StorageEngineTestSuite<PostgresqlEventStorageEngine> {
    private static PostgreSQLContainer<?> postgresContainer;
    private static DataSource dataSource;

    @SuppressWarnings("resource")
    @BeforeAll
    static void startContainer() {
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
    }

    @AfterAll
    static void stopContainer() {
        if (postgresContainer != null) {
            postgresContainer.stop();
        }
    }

    @AfterEach
    void afterEach() throws SQLException {
        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
        ) {
            statement.execute(
                """
                TRUNCATE TABLE events, consistency_tags RESTART IDENTITY CASCADE;
                ALTER SEQUENCE events_monotonic_seq RESTART WITH 1;
                """
            );

            connection.commit();
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
                throw new IllegalStateException("SQL calback failed with an SQLException", e);
            }
        }
    };

    @Override
    protected PostgresqlEventStorageEngine buildStorageEngine() throws SQLException {
        return new PostgresqlEventStorageEngine(
            connectionExecutor,
            new DelegatingEventConverter(new JacksonConverter())
        );
    }

    @Override
    protected ProcessingContext processingContext() {
        return null;
    }
}
