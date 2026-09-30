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

package org.axonframework.modelling.saga.repository.jdbc;

import org.axonframework.common.jdbc.Oracle11Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Oracle 11 does not support AUTO_INCREMENT. A workaround is used in this class in order to create the same behavior.
 * Oracle 11 does not like semicolons at the end of a CREATE TABLE statement. These semicolons are dropped in this
 * class.
 */
public class Oracle11SagaSqlSchema extends GenericSagaSqlSchema {

    /**
     * Initialize a Oracle11SagaSqlSchema using the given {@code sagaSchema}.
     *
     * @param sagaSchema the saga schema configuration
     */
    public Oracle11SagaSqlSchema(SagaSchema sagaSchema) {
        super(sagaSchema);
    }

    @Override
    public PreparedStatement sql_createTableAssocValueEntry(Connection conn) throws SQLException {
        try (PreparedStatement pst = conn.prepareStatement("create table " + sagaSchema().associationValueEntryTable() + " (\n" +
                "        id number(38) not null,\n" +
                "        " + sagaSchema.associationKeyColumn() + " varchar(255),\n" +
                "        " + sagaSchema.associationValueColumn() + " varchar(255),\n" +
                "        " + sagaSchema.sagaIdColumn() + " varchar(255),\n" +
                "        " + sagaSchema.sagaTypeColumn() + " varchar(255),\n" +
                "        primary key (id)\n" +
                "    )")) {
            pst.executeUpdate();
        }

        Oracle11Utils.simulateAutoIncrement(conn, sagaSchema().associationValueEntryTable(), "id");

        return Oracle11Utils.createNullStatement(conn);
    }

    @Override
    public PreparedStatement sql_createTableSagaEntry(final Connection conn) throws SQLException {
        return conn.prepareStatement("create table " + sagaSchema().sagaEntryTable() + " (\n" +
                "        " + sagaSchema.sagaIdColumn() + " varchar(255) not null,\n" +
                "        " + sagaSchema.revisionColumn() + " varchar(255),\n" +
                "        " + sagaSchema.sagaTypeColumn() + " varchar(255),\n" +
                "        " + sagaSchema.serializedSagaColumn() + " blob,\n" +
                "        primary key (" + sagaSchema.sagaIdColumn() + ")\n" +
                "    )");
    }
}
