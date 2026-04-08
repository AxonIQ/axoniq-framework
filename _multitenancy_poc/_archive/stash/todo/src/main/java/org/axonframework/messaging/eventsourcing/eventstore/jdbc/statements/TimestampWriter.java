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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;

/**
 * Writer interface for writing a formatted timestamp to a {@link PreparedStatement} during update of the database.
 * Used in {@link AppendEventsStatementBuilder} and {@link AppendSnapshotStatementBuilder}.
 *
 * @author Trond Marius Øvstetun
 * @since 4.4
 */
@FunctionalInterface
public interface TimestampWriter {

    /**
     * Write a timestamp from a {@link Instant} to a data value suitable for the database scheme.
     *
     * @param preparedStatement the statement to update
     * @param position          the position of the timestamp parameter in the statement
     * @param timestamp         {@link Instant} to convert
     * @throws SQLException if modification of the statement fails
     */
    void writeTimestamp(PreparedStatement preparedStatement, int position, Instant timestamp) throws SQLException;
}
