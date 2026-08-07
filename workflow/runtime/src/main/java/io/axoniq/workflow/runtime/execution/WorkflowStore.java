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

import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.concurrent.CompletableFuture;

/**
 * Narrow adapter around workflow-state repositories used during startup.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public interface WorkflowStore {

    /**
     * Loads the singleton running-workflows projection.
     *
     * @param processingContext processing context used for sourcing
     * @return current running workflow ids
     */
    @Nonnull
    CompletableFuture<RunningWorkflows> loadRunningWorkflows(@Nonnull ProcessingContext processingContext);

    /**
     * Loads the event-sourced durable state for a workflow id.
     *
     * @param workflowId        workflow identifier
     * @param processingContext processing context used for sourcing
     * @return durable workflow state
     */
    @Nonnull
    CompletableFuture<WorkflowState> loadWorkflow(@Nonnull String workflowId,
                                                  @Nonnull ProcessingContext processingContext);
}
