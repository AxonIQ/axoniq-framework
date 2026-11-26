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
 * Creates the tables and indices of a {@link JdbcWorkflowIndex}, see
 * {@link JdbcWorkflowIndex#createSchema(WorkflowIndexTableFactory,
 * org.axonframework.messaging.core.unitofwork.ProcessingContext) createSchema}.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@FunctionalInterface
public interface WorkflowIndexTableFactory {

    /**
     * Creates a {@link Statement} with a batch that creates the tables and indices of the given schema.
     *
     * @param connection the connection to create the statement with
     * @param schema     the schema defining the table and column names
     * @return a statement to create the tables and indices with, ready to be {@link Statement#executeBatch() executed}
     * @throws SQLException when the statement cannot be created
     */
    Statement createTableStatement(Connection connection, WorkflowIndexSchema schema) throws SQLException;
}
