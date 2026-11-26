/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jdbc;

/**
 * A {@link WorkflowIndexTableFactory} for PostgreSQL, which has no {@code VARBINARY} type.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
public class PostgresWorkflowIndexTableFactory extends GenericWorkflowIndexTableFactory {

    /**
     * A shared instance of the {@code PostgresWorkflowIndexTableFactory}.
     */
    public static final PostgresWorkflowIndexTableFactory INSTANCE = new PostgresWorkflowIndexTableFactory();

    /**
     * Creates a factory. Use {@link #INSTANCE} unless a subclass changes a type.
     */
    protected PostgresWorkflowIndexTableFactory() {
        super();
    }

    @Override
    protected String associationsType() {
        return "bytea";
    }
}
