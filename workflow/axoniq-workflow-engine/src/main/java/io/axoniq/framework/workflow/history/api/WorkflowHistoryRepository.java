/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.history.api;

import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Repository to access workflow history entries.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public interface WorkflowHistoryRepository {

    /**
     * Returns all stored workflow history entries matching a state query.
     *
     * @param query state criteria to apply
     * @return a future completing with an unmodifiable collection of matching workflow history entries
     */
    CompletableFuture<List<WorkflowHistory>> findAll(WorkflowStateQuery query);

    /**
     * Returns all stored workflow history entries.
     *
     * @return a future completing with an unmodifiable collection of all workflow history entries
     */
    default CompletableFuture<List<WorkflowHistory>> findAll() {
        return findAll(WorkflowStateQuery.all());
    }


    /**
     * Returns workflow history by given workflow id.
     *
     * @param workflowId the id of the workflow to retrieve history for.
     * @return a future completing with the workflow history, or an empty optional when no history exists for the given
     * workflow id
     */
    CompletableFuture<Optional<WorkflowHistory>> findById(String workflowId);
}
