/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jpa;

import io.axoniq.framework.workflow.index.suite.WorkflowIndexTestSuite;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;

/**
 * Runs the {@link WorkflowIndexTestSuite} against a {@link JpaWorkflowIndex} on an in-memory database.
 *
 * @author Stefan Dragisic
 */
class JpaWorkflowIndexTest extends WorkflowIndexTestSuite {

    private EntityManagerFactory entityManagerFactory;

    @Override
    protected WorkflowIndex createIndex() {
        // The schema is created with the factory and dropped with it, so every test starts with empty tables.
        entityManagerFactory = Persistence.createEntityManagerFactory("workflow-index");
        return new JpaWorkflowIndex(new JpaTransactionalExecutorProvider(entityManagerFactory),
                                    new JacksonConverter(),
                                    JpaWorkflowIndexConfiguration.DEFAULT);
    }

    @Override
    protected void closeIndex() {
        entityManagerFactory.close();
    }
}
