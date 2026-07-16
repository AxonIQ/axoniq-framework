/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.framework.messaging.multitenancy.eventhandling.processing.transaction.jdbc;

import org.axonframework.common.jdbc.ConnectionExecutor;
import org.axonframework.conversion.CachingSupplier;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.core.unitofwork.transaction.Transaction;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * A tenant-scoped {@link TransactionManager} for JDBC backed projections and token stores.
 * <p>
 * The manager opens one {@link Connection} per processing lifecycle, registers a
 * {@link ConnectionExecutor} supplier in the processing context, and commits or rolls back the connection together
 * with the surrounding unit of work.
 * <p>
 * This is intentionally tenant-local and should be created with the tenant's datasource.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
public class JdbcTenantTransactionManager implements TransactionManager {

    private final DataSource dataSource;

    /**
     * Creates a new tenant-local JDBC transaction manager.
     *
     * @param dataSource the datasource backing the tenant, cannot be {@code null}
     */
    public JdbcTenantTransactionManager(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource may not be null");
    }

    @Override
    public Transaction startTransaction() {
        return createTransaction(openConnection());
    }

    @Override
    public void attachToProcessingLifecycle(ProcessingLifecycle processingLifecycle) {
        processingLifecycle.runOnPreInvocation(pc -> {
            Connection connection = openConnection();
            Transaction transaction = createTransaction(connection);
            pc.putResource(
                    JdbcTransactionalExecutorProvider.SUPPLIER_KEY,
                    CachingSupplier.of(() -> new ConnectionExecutor(() -> connection))
            );
            pc.runOnCommit(p -> transaction.commit());
            pc.onError((p, phase, e) -> transaction.rollback());
        });
    }

    @Override
    public boolean requiresSameThreadInvocations() {
        return true;
    }

    private Connection openConnection() {
        try {
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to open tenant JDBC connection", e);
        }
    }

    private Transaction createTransaction(Connection connection) {
        return new Transaction() {
            private boolean completed;

            @Override
            public void commit() {
                if (!completed) {
                    completed = true;
                    try {
                        if (!connection.getAutoCommit()) {
                            connection.commit();
                        }
                    } catch (SQLException e) {
                        throw new IllegalStateException("Failed to commit tenant JDBC transaction", e);
                    } finally {
                        close(connection);
                    }
                }
            }

            @Override
            public void rollback() {
                if (!completed) {
                    completed = true;
                    try {
                        if (!connection.getAutoCommit()) {
                            connection.rollback();
                        }
                    } catch (SQLException e) {
                        throw new IllegalStateException("Failed to roll back tenant JDBC transaction", e);
                    } finally {
                        close(connection);
                    }
                }
            }
        };
    }

    private static void close(Connection connection) {
        try {
            connection.close();
        } catch (SQLException ignored) {
            // Best effort cleanup.
        }
    }
}
