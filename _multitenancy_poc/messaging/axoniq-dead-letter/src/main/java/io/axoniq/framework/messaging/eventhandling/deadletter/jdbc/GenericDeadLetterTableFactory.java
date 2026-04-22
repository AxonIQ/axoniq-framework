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
 * A {@link DeadLetterTableFactory} implementation compatible with most databases.
 *
 * @author Steven van Beelen
 * @since 4.8.0
 */
@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
public class GenericDeadLetterTableFactory implements DeadLetterTableFactory {

    @SuppressWarnings("SqlNoDataSourceInspection")
    @Override
    public Statement createTableStatement(Connection connection, DeadLetterSchema schema) throws SQLException {
        Statement statement = connection.createStatement();
        statement.addBatch(createTableSql(schema));
        statement.addBatch(processingGroupIndexSql(schema));
        statement.addBatch(sequenceIdentifierIndexSql(schema));
        return statement;
    }

    /**
     * Constructs the SQL to create a dead-letter table, using the given {@code schema} to deduce the table and column
     * names.
     *
     * @param schema The schema defining the table and column names.
     * @return The SQL to construct the dead-letter table.
     */
    protected String createTableSql(DeadLetterSchema schema) {
        return "CREATE TABLE IF NOT EXISTS " + schema.deadLetterTable() + " (\n" +
                schema.deadLetterIdentifierColumn() + " VARCHAR(255) NOT NULL,\n" +
                schema.processingGroupColumn() + " VARCHAR(255) NOT NULL,\n" +
                schema.sequenceIdentifierColumn() + " VARCHAR(255) NOT NULL,\n" +
                schema.sequenceIndexColumn() + " BIGINT NOT NULL,\n" +
                schema.eventTypeColumn() + " VARCHAR(255) NOT NULL,\n" +
                schema.eventIdentifierColumn() + " VARCHAR(255) NOT NULL,\n" +
                schema.typeColumn() + " VARCHAR(255) NOT NULL,\n" +
                schema.timestampColumn() + " " + timestampType() + " NOT NULL,\n" +
                schema.payloadColumn() + " " + serializedDataType() + " NOT NULL,\n" +
                schema.metadataColumn() + " " + serializedDataType() + ",\n" +
                schema.aggregateTypeColumn() + " VARCHAR(255),\n" +
                schema.aggregateIdentifierColumn() + " VARCHAR(255),\n" +
                schema.sequenceNumberColumn() + " BIGINT,\n" +
                schema.tokenTypeColumn() + " VARCHAR(255),\n" +
                schema.tokenColumn() + " " + serializedDataType() + ",\n" +
                schema.enqueuedAtColumn() + " " + timestampType() + " NOT NULL,\n" +
                schema.lastTouchedColumn() + " " + timestampType() + ",\n" +
                schema.processingStartedColumn() + " " + timestampType() + ",\n" +
                schema.causeTypeColumn() + " VARCHAR(255),\n" +
                schema.causeMessageColumn() + " VARCHAR(1023),\n" +
                schema.diagnosticsColumn() + " " + serializedDataType() + ",\n" +
                "CONSTRAINT PK PRIMARY KEY (" + schema.deadLetterIdentifierColumn() + "),\n" +
                "CONSTRAINT " + schema.sequenceIndexColumn() + "_INDEX UNIQUE (" +
                schema.processingGroupColumn() + "," +
                schema.sequenceIdentifierColumn() + "," +
                schema.sequenceIndexColumn() +
                ")\n)";
    }

    /**
     * Constructs the SQL to create an index of the {@link DeadLetterSchema#processingGroupColumn() processing group} ,
     * using the given {@code schema} to deduce the table and column names.
     *
     * @param schema The schema defining the table and column names.
     * @return The SQL to construct the index for the {@link DeadLetterSchema#processingGroupColumn() processing group}
     * for the dead-letter table.
     */
    protected String processingGroupIndexSql(DeadLetterSchema schema) {
        return "CREATE INDEX " + schema.processingGroupColumn() + "_INDEX "
                + "ON " + schema.deadLetterTable() + " "
                + "(" + schema.processingGroupColumn() + ")";
    }

    /**
     * Constructs the SQL to create an index for the {@link DeadLetterSchema#processingGroupColumn() processing group}
     * and {@link DeadLetterSchema#sequenceIdentifierColumn() sequence indentifier} combination, using the given
     * {@code schema} to deduce the table and column names.
     *
     * @param schema The schema defining the table and column names.
     * @return The SQL to construct the index for the {@link DeadLetterSchema#processingGroupColumn() processing group} and
     * {@link DeadLetterSchema#sequenceIdentifierColumn() combination} for the dead-letter table.
     */
    protected String sequenceIdentifierIndexSql(DeadLetterSchema schema) {
        return "CREATE INDEX " + schema.sequenceIdentifierColumn() + "_INDEX "
                + "ON " + schema.deadLetterTable() + " "
                + "(" + schema.processingGroupColumn() + "," + schema.sequenceIdentifierColumn() + ")";
    }

    /**
     * Returns the SQL to describe the type for serialized data columns.
     * <p>
     * Used for the {@link DeadLetterSchema#payloadColumn()}, {@link DeadLetterSchema#metadataColumn()},
     * {@link DeadLetterSchema#tokenColumn()}, and the {@link DeadLetterSchema#diagnosticsColumn()}. Defaults to
     * {@code BLOB}.
     *
     * @return The SQL to describe the type for serialized data columns.
     */
    protected String serializedDataType() {
        return "BLOB";
    }

    /**
     * Returns the SQL to describe the type for timestamp columns.
     * <p>
     * Used for the {@link DeadLetterSchema#enqueuedAtColumn()}, {@link DeadLetterSchema#lastTouchedColumn()},
     * {@link DeadLetterSchema#processingGroupColumn()}, and the {@link DeadLetterSchema#timestampColumn()}. Defaults to
     * {@code VARCHAR(255)}.
     *
     * @return The SQL to describe the type for timestamp columns.
     */
    protected String timestampType() {
        return "VARCHAR(255)";
    }
}
