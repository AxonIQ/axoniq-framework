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

package org.axonframework.messaging.eventsourcing.eventstore.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Interface describing a factory for JDBC to create the Domain and Snapshot Event tables.
 *
 * @author Rene de Waele
 * @since 3.0
 */
public interface EventTableFactory {

    /**
     * Creates a PreparedStatement that allows for the creation of the table to store Event entries.
     *
     * @param connection The connection to create the PreparedStatement for
     * @param schema     The event schema with the name of the table and its columns
     * @return The statement to create the table, ready to be executed
     *
     * @throws SQLException when an exception occurs while creating the prepared statement
     */
    PreparedStatement createDomainEventTable(Connection connection, EventSchema schema) throws SQLException;

    /**
     * Creates a PreparedStatement that allows for the creation of the table to store Snapshots.
     *
     * @param connection The connection to create the PreparedStatement for
     * @param schema     The event schema with the name of the table and its columns
     * @return The statement to create the table, ready to be executed
     *
     * @throws SQLException when an exception occurs while creating the prepared statement
     */
    PreparedStatement createSnapshotEventTable(Connection connection, EventSchema schema) throws SQLException;
}
