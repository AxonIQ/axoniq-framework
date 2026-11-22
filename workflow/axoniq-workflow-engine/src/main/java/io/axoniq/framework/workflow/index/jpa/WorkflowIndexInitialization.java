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
import org.axonframework.common.annotation.Internal;

import static java.util.Objects.requireNonNull;

/**
 * Marks an index of a {@link JpaWorkflowIndex} as initialized, identified by the name of the index.
 * <p>
 * Without this row, the index reports that it was not initialized, whatever entries it holds, and a claim reads the
 * running workflow instances from the event store instead. See
 * {@link io.axoniq.framework.workflow.index.api.WorkflowIndex} for why an index needs to be initialized.
 * <p>
 * The entity is part of the database schema of the {@link JpaWorkflowIndex}: include it in the persistence unit that
 * index uses. It is not meant to be read or written by application code.
 * <p>
 * Marked {@link Internal} as a warning that API stability is not promised yet: this entity is usable, and supported
 * as part of the schema of the {@link JpaWorkflowIndex}, but its shape may still change in a minor release.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
@Entity
public class WorkflowIndexInitialization {

    @Id
    private String indexName;

    /**
     * Marks the index with the given name as initialized.
     *
     * @param indexName the name of the initialized index
     */
    public WorkflowIndexInitialization(String indexName) {
        this.indexName = requireNonNull(indexName, "The index name must not be null.");
    }

    /**
     * Constructor for JPA.
     */
    protected WorkflowIndexInitialization() {
    }

    /**
     * Returns the name of the initialized index.
     *
     * @return the name of the index
     */
    public String getIndexName() {
        return indexName;
    }
}
