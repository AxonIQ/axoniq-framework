/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.api;

import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import jakarta.annotation.Nonnull;

/**
 * Represents a running or completed workflow instance, bundling its configuration,
 * context, and execution state.
 *
 * @param workflowConfiguration the workflow configuration that created this instance
 * @param workflowContext       the context holding workflow data and status
 * @param workflowState         the execution state machine for this workflow
 */
public record WorkflowExecution(
        @Nonnull String workflowId,
        @Nonnull WorkflowConfiguration<?> workflowConfiguration,
        @Nonnull  WorkflowContext workflowContext,
        @Nonnull WorkflowState workflowState) {

    public WorkflowStatus getStatus() {
        return workflowContext.getStatus();
    }
    //todo entity
    //todo implement equals by workflow id
}
