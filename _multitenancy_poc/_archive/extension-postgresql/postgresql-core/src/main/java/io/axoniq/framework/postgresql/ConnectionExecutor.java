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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotations.Internal;
import org.axonframework.messaging.unitofwork.ProcessingContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Executes JDBC operations with automatic transaction management and optional
 * integration with a {@link ProcessingContext}.
 * <p>
 * The primary purpose of this interface is to allow callers to execute JDBC logic
 * without needing to manage commits, rollbacks or closing of the connection manually.
 * Errors will result in a rollback, and successful execution will commit or leave
 * the connection in a default consistent state.
 * <p>
 * The user of this interface must never explicitely call {@link Connection#close()},
 * {@link Connection#commit()} or {@link Connection#rollback()} on the provided connection!
 * <p>
 * Implementations can integrate with different connection sources or transaction
 * managers, such as:
 * <ul>
 *     <li>Plain JDBC connections</li>
 *     <li>Connection pools (e.g., HikariCP)</li>
 *     <li>Spring-managed transactions</li>
 * </ul>
 *
 * <p>This interface provides convenience methods for both operations that return
 * a result and operations that are purely side-effecting.
 *
 * @author John Hendrikx
 * @since 0.1.0
 */
@Internal
public interface ConnectionExecutor {

    /**
     * Executes a JDBC operation that does not return a result.
     * <p>
     * Suitable for operations that only perform actions (INSERT, UPDATE, DELETE, etc.).
     *
     * @param context optional processing context, can be {@code null}
     * @param consumer a JDBC consumer that accepts a {@link Connection} and may throw {@link SQLException},
     *     cannot be {@code null}
     * @throws NullPointerException when {@code consumer} is {@code null}
     */
    default void execute(@Nullable ProcessingContext context, @Nonnull JdbcConsumer<Connection> consumer) {
        Objects.requireNonNull(consumer, "consumer");

        execute(context, c -> {
            consumer.accept(c);

            return null;
        });
    }

    /**
     * Executes a JDBC operation that returns a result.
     * <p>
     * Implementations are responsible for providing the connection, handling
     * transactions, and closing resources. The function may throw {@link SQLException},
     * which should be propagated appropriately by the executor.
     *
     * @param <R> the type of result returned by the function
     * @param context optional processing context, can be {@code null}
     * @param function a JDBC function that accepts a {@link Connection} and produces a result,
     *     cannot be {@code null}
     * @return the result produced by the function
     * @throws NullPointerException when {@code function} is {@code null}
     */
    <R> R execute(@Nullable ProcessingContext context, @Nonnull JdbcFunction<Connection, R> function);

    /**
     * Functional interface for JDBC operations which may throw an {@link SQLException} that
     * consume a {@link Connection} and return no result.
     */
    interface JdbcConsumer<T> {

        /**
         * Accepts a JDBC input and performs an operation.
         *
         * @param input the JDBC input, typically a {@link Connection}
         * @throws SQLException if a database access error occurs
         */
        void accept(T input) throws SQLException;
    }

    /**
     * Functional interface for JDBC operations which may throw an {@link SQLException} that
     * consume a {@link Connection} and return a result.
     *
     * @param <T> the type of the input, typically {@link Connection}
     * @param <R> the type of the result returned by the function
     */
    interface JdbcFunction<T, R> {

        /**
         * Applies the function to the JDBC input.
         *
         * @param input the JDBC input, typically a {@link Connection}
         * @return the result of the operation
         * @throws SQLException if a database access error occurs
         */
        R apply(T input) throws SQLException;
    }
}

