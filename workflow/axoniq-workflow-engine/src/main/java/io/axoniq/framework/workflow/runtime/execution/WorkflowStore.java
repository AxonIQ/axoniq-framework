/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Narrow adapter around workflow-state repositories used during startup.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public interface WorkflowStore {

    /**
     * Loads the singleton running-workflows projection.
     *
     * @param processingContext processing context used for sourcing
     * @return current running workflow ids
     */
    CompletableFuture<RunningWorkflows> loadRunningWorkflows(ProcessingContext processingContext);

    /**
     * Loads the event-sourced durable state for a workflow id.
     *
     * @param workflowId        workflow identifier
     * @param processingContext processing context used for sourcing
     * @return durable workflow state
     */
    CompletableFuture<WorkflowState> loadWorkflow(String workflowId,
                                                  ProcessingContext processingContext);

    /**
     * Sources the state of a workflow that may not exist.
     *
     * @param workflowId        identifier of the workflow to source
     * @param processingContext processing context the sourcing runs in
     * @return a future completing with the sourced state, or empty when the store holds no events for the workflow
     */
    CompletableFuture<Optional<WorkflowState>> findWorkflow(String workflowId,
                                                            ProcessingContext processingContext);
}
