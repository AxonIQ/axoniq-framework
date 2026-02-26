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
import org.axonframework.common.annotation.Internal;

/**
 * Represents a running or completed workflow instance, bundling its configuration,
 * context, and execution state.
 *
 * @param workflowConfiguration the workflow configuration that created this instance
 * @param workflowContext       the context holding workflow data and status
 * @param workflowState         the execution state machine for this workflow
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@Internal
public record WorkflowExecution(
        @Nonnull String workflowId,
        @Nonnull WorkflowConfiguration<?> workflowConfiguration,
        @Nonnull  WorkflowContext workflowContext,
        @Nonnull WorkflowState workflowState) {

    /**
     * Returns the current execution status of this workflow instance.
     *
     * @return the current {@link WorkflowStatus}
     */
    public WorkflowStatus getStatus() {
        return workflowContext.workflowStatus();
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkflowExecution that)) return false;
        return workflowId.equals(that.workflowId);
    }

    @Override
    public int hashCode() {
        return workflowId.hashCode();
    }
}
