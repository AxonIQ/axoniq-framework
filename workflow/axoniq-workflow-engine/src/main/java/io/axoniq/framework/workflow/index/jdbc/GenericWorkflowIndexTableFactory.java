/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A {@link WorkflowIndexTableFactory} compatible with most databases. The tables match those the
 * {@link io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex} maps with the default JPA naming, so an application
 * can switch between the two.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
public class GenericWorkflowIndexTableFactory implements WorkflowIndexTableFactory {

    /**
     * A shared instance of the {@code GenericWorkflowIndexTableFactory}.
     */
    public static final GenericWorkflowIndexTableFactory INSTANCE = new GenericWorkflowIndexTableFactory();

    /**
     * Creates a factory. Use {@link #INSTANCE} unless a subclass changes a type.
     */
    protected GenericWorkflowIndexTableFactory() {
    }

    @Override
    public Statement createTableStatement(Connection connection, WorkflowIndexSchema schema) throws SQLException {
        Statement statement = connection.createStatement();
        statement.addBatch("CREATE TABLE IF NOT EXISTS " + schema.workflowTable() + " (\n"
                                   + schema.workflowModuleColumn() + " VARCHAR(255) NOT NULL,\n"
                                   + schema.workflowIdColumn() + " VARCHAR(255) NOT NULL,\n"
                                   + schema.segmentKeyColumn() + " BIGINT NOT NULL,\n"
                                   + schema.wakesAtColumn() + " VARCHAR(255),\n"
                                   + "PRIMARY KEY (" + schema.workflowModuleColumn() + ","
                                   + schema.workflowIdColumn() + ")\n)");
        statement.addBatch("CREATE TABLE IF NOT EXISTS " + schema.waitTable() + " (\n"
                                   + schema.waitIdColumn() + " VARCHAR(255) NOT NULL,\n"
                                   + schema.workflowModuleColumn() + " VARCHAR(255) NOT NULL,\n"
                                   + schema.workflowIdColumn() + " VARCHAR(255) NOT NULL,\n"
                                   + schema.stepNameColumn() + " VARCHAR(1024) NOT NULL,\n"
                                   + schema.segmentKeyColumn() + " BIGINT NOT NULL,\n"
                                   + schema.eventNameColumn() + " VARCHAR(255),\n"
                                   + schema.associationsColumn() + " " + associationsType() + ",\n"
                                   + "PRIMARY KEY (" + schema.waitIdColumn() + ")\n)");
        statement.addBatch("CREATE TABLE IF NOT EXISTS " + schema.initializationTable() + " (\n"
                                   + schema.workflowModuleColumn() + " VARCHAR(255) NOT NULL,\n"
                                   + "PRIMARY KEY (" + schema.workflowModuleColumn() + ")\n)");
        statement.addBatch("CREATE TABLE IF NOT EXISTS " + schema.positionTable() + " (\n"
                                   + schema.workflowModuleColumn() + " VARCHAR(255) NOT NULL,\n"
                                   + schema.segmentFromColumn() + " BIGINT NOT NULL,\n"
                                   + schema.segmentToColumn() + " BIGINT NOT NULL,\n"
                                   + schema.segmentPositionColumn() + " BIGINT NOT NULL,\n"
                                   + "PRIMARY KEY (" + schema.workflowModuleColumn() + ","
                                   + schema.segmentFromColumn() + "," + schema.segmentToColumn() + ")\n)");
        statement.addBatch("CREATE INDEX " + schema.workflowTable() + "_segment ON " + schema.workflowTable()
                                   + " (" + schema.workflowModuleColumn() + "," + schema.segmentKeyColumn() + ")");
        statement.addBatch("CREATE INDEX " + schema.waitTable() + "_segment ON " + schema.waitTable()
                                   + " (" + schema.workflowModuleColumn() + "," + schema.segmentKeyColumn() + ")");
        statement.addBatch("CREATE INDEX " + schema.waitTable() + "_workflow ON " + schema.waitTable()
                                   + " (" + schema.workflowModuleColumn() + "," + schema.workflowIdColumn() + ")");
        return statement;
    }

    /**
     * Returns the SQL type of the associations column, which holds at most 10,000 bytes. Defaults to
     * {@code VARBINARY(10000)}.
     *
     * @return the SQL type of the associations column
     */
    protected String associationsType() {
        return "VARBINARY(10000)";
    }
}
