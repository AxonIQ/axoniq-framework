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
import org.axonframework.modelling.repository.ManagedEntity;
import org.axonframework.modelling.repository.Repository;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Event-sourcing repository-backed workflow store.
 * <p>
 * Loads the projection of workflow instances that have started without reaching a terminal state and the durable
 * event-sourced state for an individual workflow instance.
 * </p>
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class EventSourcedWorkflowStore implements WorkflowStore {

    private final Repository<String, EventSourcedRunningWorkflows> runningWorkflowsRepository;
    private final Repository<String, EventSourcedWorkflowState> workflowStateRepository;

    public EventSourcedWorkflowStore(
            Repository<String, EventSourcedRunningWorkflows> runningWorkflowsRepository,
            Repository<String, EventSourcedWorkflowState> workflowStateRepository
    ) {
        this.runningWorkflowsRepository = Objects.requireNonNull(runningWorkflowsRepository,
                                                                 "Running workflows repository must not be null");
        this.workflowStateRepository = Objects.requireNonNull(workflowStateRepository,
                                                              "Workflow state repository must not be null");
    }

    @Override
    public CompletableFuture<RunningWorkflows> loadRunningWorkflows(ProcessingContext processingContext) {
        return runningWorkflowsRepository.loadOrCreate(EventSourcedRunningWorkflows.ENTITY_ID, processingContext)
                                         .thenApply(ManagedEntity::entity);
    }

    @Override
    public CompletableFuture<WorkflowState> loadWorkflow(String workflowId,
                                                         ProcessingContext processingContext) {
        return workflowStateRepository.loadOrCreate(workflowId, processingContext)
                                      .thenApply(ManagedEntity::entity);
    }

    @Override
    public CompletableFuture<Optional<WorkflowState>> findWorkflow(String workflowId,
                                                                   ProcessingContext processingContext) {
        return workflowStateRepository.load(workflowId, processingContext)
                                      .thenApply(managed -> Optional.ofNullable(managed.entity()));
    }
}
