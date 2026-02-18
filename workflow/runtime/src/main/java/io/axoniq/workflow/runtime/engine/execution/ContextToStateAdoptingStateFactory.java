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
package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.api.WorkflowContext;
import jakarta.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

import static io.axoniq.workflow.runtime.engine.execution.WorkflowState.requireIsWorkflowState;

public class ContextToStateAdoptingStateFactory<C extends WorkflowContext> implements WorkflowStateFactory {

    private final Class<C> workflowContextType;

    public ContextToStateAdoptingStateFactory(@Nonnull Class<C> workflowContextType) {
        this.workflowContextType = requireIsWorkflowState(workflowContextType);
    }

    @Override
    @Nonnull
    public WorkflowState create(@NotNull WorkflowContext context) {
        if (workflowContextType.isAssignableFrom(context.getClass())) {
            return (WorkflowState) context;
        }
        throw new IllegalStateException("Unsupported context type " + context.getClass().getName());
    }
}
