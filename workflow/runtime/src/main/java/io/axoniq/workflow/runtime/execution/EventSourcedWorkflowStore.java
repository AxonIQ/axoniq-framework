/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.workflow.runtime.execution;

import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.modelling.repository.ManagedEntity;
import org.axonframework.modelling.repository.Repository;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Axon Framework repository-backed workflow store.
 * <p>
 * Loads the projection of workflow instances that have started without reaching a terminal state and the durable
 * event-sourced state for an individual workflow instance.
 * </p>
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class EventSourcedWorkflowStore implements WorkflowStore {

    private final Repository<String, EventSourcedRunningWorkflows> runningWorkflowsRepository;
    private final Repository<String, EventSourcedWorkflowState> workflowStateRepository;

    public EventSourcedWorkflowStore(
            @Nonnull Repository<String, EventSourcedRunningWorkflows> runningWorkflowsRepository,
            @Nonnull Repository<String, EventSourcedWorkflowState> workflowStateRepository
    ) {
        this.runningWorkflowsRepository = Objects.requireNonNull(runningWorkflowsRepository,
                                                                 "Running workflows repository must not be null");
        this.workflowStateRepository = Objects.requireNonNull(workflowStateRepository,
                                                              "Workflow state repository must not be null");
    }

    @Override
    @Nonnull
    public CompletableFuture<RunningWorkflows> loadRunningWorkflows(@Nonnull ProcessingContext processingContext) {
        return runningWorkflowsRepository.loadOrCreate(EventSourcedRunningWorkflows.ENTITY_ID, processingContext)
                                         .thenApply(ManagedEntity::entity);
    }

    @Override
    @Nonnull
    public CompletableFuture<EventSourcedWorkflowState> loadWorkflow(@Nonnull String workflowId,
                                                                     @Nonnull ProcessingContext processingContext) {
        return workflowStateRepository.loadOrCreate(workflowId, processingContext)
                                      .thenApply(ManagedEntity::entity);
    }
}
