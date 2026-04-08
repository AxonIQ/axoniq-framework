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

package org.axonframework.messaging.eventsourcing.eventstore.jdbc.statements;

import org.axonframework.messaging.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.messaging.eventsourcing.eventstore.jdbc.LegacyJdbcEventStorageEngine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Contract which defines how to build a PreparedStatement for use on
 * {@link LegacyJdbcEventStorageEngine#createTailToken()}
 *
 * @author Lucas Campos
 * @since 4.3
 */
@FunctionalInterface
public interface CreateTailTokenStatementBuilder {

    /**
     * Creates a statement to be used at {@link LegacyJdbcEventStorageEngine#createTailToken()}.
     *
     * @param connection The connection to the database.
     * @param schema     The EventSchema to be used
     * @return The newly created {@link PreparedStatement}.
     * @throws SQLException when an exception occurs while creating the prepared statement.
     */
    PreparedStatement build(Connection connection, EventSchema schema) throws SQLException;
}
