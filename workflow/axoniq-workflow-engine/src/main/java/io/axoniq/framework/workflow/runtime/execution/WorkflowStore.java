/*
 * Copyright (c) 2010-2026. Axoniq B.V.
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
 * <p>
 * It also keeps the {@link HibernatedWorkflows} the engine records for each segment, when the store can keep them.
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
     * Loads the singleton wait-associations projection.
     *
     * @param processingContext processing context used for sourcing
     * @return the open waits of every workflow
     */
    CompletableFuture<WaitAssociations> loadWaitAssociations(ProcessingContext processingContext);

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

    /**
     * Loads the hibernated workflows last recorded under the given identifier. A store that cannot keep them returns
     * {@link HibernatedWorkflows#NONE}, so every workflow of a claimed segment is restored.
     *
     * @param identifier        the identifier the engine recorded them under, one per engine and segment
     * @param processingContext processing context the load runs in
     * @return the recorded hibernated workflows, or {@link HibernatedWorkflows#NONE} when none are recorded
     */
    default CompletableFuture<HibernatedWorkflows> loadHibernatedWorkflows(String identifier,
                                                                            ProcessingContext processingContext) {
        return CompletableFuture.completedFuture(HibernatedWorkflows.NONE);
    }

    /**
     * Records the given hibernated workflows under the given identifier, replacing the earlier record. A store that
     * cannot keep them ignores the call.
     *
     * @param identifier        the identifier to record them under, one per engine and segment
     * @param workflows         the hibernated workflows
     * @param processingContext processing context the store runs in
     * @return a future completing once the record is stored
     */
    default CompletableFuture<Void> storeHibernatedWorkflows(String identifier,
                                                             HibernatedWorkflows workflows,
                                                             ProcessingContext processingContext) {
        return CompletableFuture.completedFuture(null);
    }
}
