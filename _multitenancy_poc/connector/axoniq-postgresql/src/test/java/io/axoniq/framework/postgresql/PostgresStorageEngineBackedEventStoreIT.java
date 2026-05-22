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
import org.axonframework.common.jdbc.ConnectionExecutor;
import org.axonframework.common.jdbc.ConnectionProvider;
import org.axonframework.conversion.CachingSupplier;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStoreTestSuite;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;

public class PostgresStorageEngineBackedEventStoreIT extends StorageEngineBackedEventStoreTestSuite<PostgresqlEventStorageEngine> {
    private static final UnitOfWorkFactory FACTORY = new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE);

    private static PostgreSQLContainer postgresContainer;
    private static DataSource dataSource;

    private static PostgresqlEventStorageEngine engine;

    @SuppressWarnings("resource")
    @BeforeAll
    static void buildEngine() {
        postgresContainer = new PostgreSQLContainer("postgres:16.2")
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
        if (engine != null) {
            engine.close();
        }

        if (postgresContainer != null) {
            postgresContainer.stop();
        }
    }

    @Nested
    class OverrideAppendCondition
            extends StorageEngineBackedEventStoreTestSuite<PostgresqlEventStorageEngine>.OverrideAppendCondition {

        @Override
        @Disabled("Type based Append conditions not yet implemented")
        @Test
        protected void narrowedCriteriaShouldAvoidFalseConflict() {
            // TODO enable once #52 is fixed
        }
    }

    @Override
    protected PostgresqlEventStorageEngine getStorageEngine(EventConverter converter) {
        if (engine == null) {
            engine = new PostgresqlEventStorageEngine(dataSource, converter);
        }

        return engine;
    }

    @Override
    protected UnitOfWork unitOfWork() {
        UnitOfWork unitOfWork = FACTORY.create();

        var cp = new ConnectionProvider() {
            private Connection obtainedConnection;

            @Override
            public synchronized Connection getConnection() throws SQLException {
                if (obtainedConnection == null) {
                    obtainedConnection = dataSource.getConnection();

                    obtainedConnection.setAutoCommit(false);
                }

                return obtainedConnection;
            }

            synchronized void commit() throws SQLException {
                if (obtainedConnection != null) {
                    obtainedConnection.commit();
                }
            }

            synchronized void rollback() throws SQLException {
                if (obtainedConnection != null) {
                    obtainedConnection.rollback();
                }
            }

            synchronized void close() {
                try {
                    if (obtainedConnection != null) {
                        obtainedConnection.close();
                    }
                }
                catch (SQLException e) {
                    // Ignore, if closing fails, the system is likely in a bad state already
                }
            }
        };

        unitOfWork.runOnPreInvocation(pc -> {
            pc.putResource(
                JdbcTransactionalExecutorProvider.SUPPLIER_KEY,
                CachingSupplier.of(() -> new ConnectionExecutor(cp))
            );

            pc.onCommit(p -> {
                try {
                    cp.commit();

                    return CompletableFuture.completedFuture(null);
                }
                catch (SQLException e) {
                    return CompletableFuture.failedFuture(e);
                }
                finally {
                    cp.close();
                }
            });

            pc.onError((p, phase, e) -> {
                try {
                    cp.rollback();
                }
                catch (SQLException se) {
                    e.addSuppressed(se);
                }
                finally {
                    cp.close();
                }
            });
        });

        return unitOfWork;
    }

}
