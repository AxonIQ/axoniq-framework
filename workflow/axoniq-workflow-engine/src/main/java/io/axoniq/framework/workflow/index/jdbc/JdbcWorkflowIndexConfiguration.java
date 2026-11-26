/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jdbc;

import java.util.Objects;

/**
 * Configuration of a {@link JdbcWorkflowIndex}.
 *
 * @param storesWithToken whether the rows are stored in the transaction of the token store, so they commit with the
 *                        token; {@code false} when the index lives in another database
 * @param schema          the table and column names of the index
 * @author Stefan Dragisic
 * @since 5.5.0
 */
public record JdbcWorkflowIndexConfiguration(boolean storesWithToken, WorkflowIndexSchema schema) {

    /**
     * The default configuration, which stores the rows in the transaction of the token store, in the tables of the
     * {@link WorkflowIndexSchema#defaultSchema() default schema}.
     */
    public static final JdbcWorkflowIndexConfiguration DEFAULT =
            new JdbcWorkflowIndexConfiguration(true, WorkflowIndexSchema.defaultSchema());

    /**
     * Creates a configuration.
     *
     * @param storesWithToken whether rows and token are committed in one transaction
     * @param schema          the table and column names of the index
     */
    public JdbcWorkflowIndexConfiguration {
        Objects.requireNonNull(schema, "The schema must not be null.");
    }

    /**
     * Sets whether the rows are stored in the transaction of the token store. Defaults to {@code true}. With
     * {@code false}, the engine keeps no wake times, so a segment claim loads every running workflow.
     *
     * @param storesWithToken whether rows and token are committed in one transaction
     * @return a copy of this configuration with the given setting
     */
    public JdbcWorkflowIndexConfiguration storesWithToken(boolean storesWithToken) {
        return new JdbcWorkflowIndexConfiguration(storesWithToken, schema);
    }

    /**
     * Sets the table and column names of the index. Defaults to the {@link WorkflowIndexSchema#defaultSchema()}.
     *
     * @param schema the table and column names to use
     * @return a copy of this configuration with the given schema
     */
    public JdbcWorkflowIndexConfiguration schema(WorkflowIndexSchema schema) {
        return new JdbcWorkflowIndexConfiguration(storesWithToken, schema);
    }
}
