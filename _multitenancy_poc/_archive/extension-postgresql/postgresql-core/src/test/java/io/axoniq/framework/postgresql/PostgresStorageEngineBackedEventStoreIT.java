/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.framework.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.axonframework.common.jdbc.ConnectionExecutor;
import org.axonframework.common.jdbc.ConnectionProvider;
import org.axonframework.conversion.CachingSupplier;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStoreTestSuite;
import org.axonframework.eventsourcing.eventstore.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

import javax.sql.DataSource;

public class PostgresStorageEngineBackedEventStoreIT extends StorageEngineBackedEventStoreTestSuite<PostgresqlEventStorageEngine> {
    private static final UnitOfWorkFactory FACTORY = new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE);

    private static PostgreSQLContainer<?> postgresContainer;
    private static DataSource dataSource;

    private static PostgresqlEventStorageEngine engine;

    @SuppressWarnings("resource")
    @BeforeAll
    static void buildEngine() {
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
        if (engine != null) {
            engine.close();
        }

        if (postgresContainer != null) {
            postgresContainer.stop();
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
