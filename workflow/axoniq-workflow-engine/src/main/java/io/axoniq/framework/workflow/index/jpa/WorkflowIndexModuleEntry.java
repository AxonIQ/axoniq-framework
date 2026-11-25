/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jpa;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import java.util.Objects;

/**
 * Marks the rows of one workflow module in the {@link JpaWorkflowIndex} as complete: they hold every running workflow
 * and every open wait of the module, so a segment claim may read them.
 * <p>
 * A module without this row has rows that started with an empty index, while workflows may have been running already.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Entity
public class WorkflowIndexModuleEntry {

    @Id
    @Column(length = 255)
    private String workflowModule;

    /**
     * Constructor required by JPA. Do not use.
     */
    protected WorkflowIndexModuleEntry() {
        // required by JPA
    }

    /**
     * Creates the row marking the given workflow module as complete.
     *
     * @param workflowModule the name of the workflow module
     */
    public WorkflowIndexModuleEntry(String workflowModule) {
        this.workflowModule = Objects.requireNonNull(workflowModule, "The workflow module must not be null.");
    }

    /**
     * Returns the name of the workflow module.
     *
     * @return the name of the workflow module
     */
    public String getWorkflowModule() {
        return workflowModule;
    }
}
