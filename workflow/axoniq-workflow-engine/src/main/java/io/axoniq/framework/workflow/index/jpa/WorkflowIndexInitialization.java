/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jpa;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import java.util.Objects;

/**
 * Marks the rows of one workflow module in the {@link JpaWorkflowIndex} as initialized. Without this row, the index
 * loads nothing for the module, whatever rows it holds, and a claim sources the running workflows and the open waits
 * instead, and initializes the index with them.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Entity
public class WorkflowIndexInitialization {

    @Id
    private String workflowModule;

    /**
     * Constructor required by JPA. Do not use.
     */
    protected WorkflowIndexInitialization() {
    }

    /**
     * Marks the rows of the given workflow module as initialized.
     *
     * @param workflowModule the name of the workflow module
     */
    public WorkflowIndexInitialization(String workflowModule) {
        this.workflowModule = Objects.requireNonNull(workflowModule, "The workflow module must not be null.");
    }

    /**
     * Returns the name of the initialized workflow module.
     *
     * @return the name of the workflow module
     */
    public String getWorkflowModule() {
        return workflowModule;
    }
}
