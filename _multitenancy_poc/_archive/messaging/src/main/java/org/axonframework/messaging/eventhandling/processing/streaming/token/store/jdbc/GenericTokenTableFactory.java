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

package org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Jdbc token entry table factory compatible with most databases.
 *
 * @author Rene de Waele
 */
public class GenericTokenTableFactory implements TokenTableFactory {

    /**
     * Creates a singleton reference the GenericTokenTableFactory implementation.
     */
    public static final GenericTokenTableFactory INSTANCE = new GenericTokenTableFactory();

    protected GenericTokenTableFactory() {
    }

    @Override
    public PreparedStatement createTable(Connection connection, TokenSchema schema) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS " + schema.tokenTable() + " (\n" +
                schema.processorNameColumn() + " VARCHAR(255) NOT NULL,\n" +
                schema.segmentColumn() + " INTEGER NOT NULL,\n" +
                schema.maskColumn() + " INTEGER NOT NULL,\n" +
                schema.tokenColumn() + " " + tokenType() + " NULL,\n" +
                schema.tokenTypeColumn() + " VARCHAR(255) NULL,\n" +
                schema.timestampColumn() + " VARCHAR(255) NULL,\n" +
                schema.ownerColumn() + " VARCHAR(255) NULL,\n" +
                "PRIMARY KEY (" + schema.processorNameColumn() + "," + schema.segmentColumn() + ")\n" +
                ")";
        return connection.prepareStatement(sql);
    }

    /**
     * Returns the sql to describe the type of token column.
     *
     * @return the sql for the token column
     */
    protected String tokenType() {
        return "BLOB";
    }
}
