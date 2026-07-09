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
import org.axonframework.modelling.repository.Repository;

import java.util.Objects;

/**
 * Event-sourcing repository backed implementation of workflow-state rehydration support.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class RepositoryBackedWorkflowStateRehydrationSupport implements WorkflowStateRehydrationSupport {

    private final Repository<String, RunningWorkflows> runningWorkflowsRepository;
    private final Repository<String, EventSourcedWorkflowState> workflowStateRepository;

    public RepositoryBackedWorkflowStateRehydrationSupport(
            @Nonnull Repository<String, RunningWorkflows> runningWorkflowsRepository,
            @Nonnull Repository<String, EventSourcedWorkflowState> workflowStateRepository
    ) {
        this.runningWorkflowsRepository = Objects.requireNonNull(runningWorkflowsRepository,
                                                                 "Running workflows repository must not be null");
        this.workflowStateRepository = Objects.requireNonNull(workflowStateRepository,
                                                              "Workflow state repository must not be null");
    }

    @Override
    @Nonnull
    public RunningWorkflows loadRunningWorkflows(@Nonnull ProcessingContext processingContext) {
        return runningWorkflowsRepository.loadOrCreate(RunningWorkflows.ENTITY_ID, processingContext)
                                         .join()
                                         .entity();
    }

    @Override
    @Nonnull
    public EventSourcedWorkflowState loadWorkflowState(@Nonnull String workflowId,
                                                       @Nonnull ProcessingContext processingContext) {
        return workflowStateRepository.loadOrCreate(workflowId, processingContext)
                                      .join()
                                      .entity();
    }
}
