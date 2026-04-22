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

package io.axoniq.framework.messaging.eventhandling.deadletter.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A functional interface to create a JDBC-specific {@link io.axoniq.framework.messaging.deadletter.DeadLetter} entry
 * table and its indices.
 *
 * @author Steven van Beelen
 * @since 4.8.0
 */
@FunctionalInterface
public interface DeadLetterTableFactory {

    /**
     * Creates a {@link Statement} to use for construction of a
     * {@link io.axoniq.framework.messaging.deadletter.DeadLetter} entry table and its indices.
     * <p>
     * The returned {@code Statement} typically contains several SQL statements and hence the invoker is inclined to
     * execute the {@code Statement} as a batch by invoking {@link Statement#executeBatch()}. Furthermore, it is
     * expected that this statement at least constructs the required uniqueness constraints.
     *
     * @param connection The connection to create the {@link Statement} with.
     * @param schema     The schema defining the table and column names.
     * @return A {@link Statement statement} to create the table and its indices with, ready to be
     * {@link Statement#executeBatch() executed}.
     * @throws SQLException when an exception occurs while creating the {@link Statement}.
     */
    Statement createTableStatement(Connection connection, DeadLetterSchema schema) throws SQLException;
}
