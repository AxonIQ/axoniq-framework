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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import jakarta.annotation.Nonnull;

import static io.axoniq.workflow.runtime.util.WorkflowReflectionUtils.requireIsAssignableFrom;

/**
 * Workflow execution factory adopting a DSL instance implementing {@link AbstractDSLWorkflowContext}.
 *
 * @param <C> type of the implementation.
 * @author Simon Zambrovki
 * @since 1.0.0
 */
public class DSLAdoptingExecutionFactory<C extends WorkflowContext> implements
        WorkflowExecutionFactory {

    private final Class<C> workflowContextType;

    /**
     * Creates a new factory.
     *
     * @param workflowContextType type of the workflow context.
     */
    public DSLAdoptingExecutionFactory(@Nonnull Class<C> workflowContextType) {
        this.workflowContextType = requireIsAssignableFrom(AbstractDSLWorkflowContext.class, workflowContextType);
    }

    @Override
    @Nonnull
    public WorkflowExecution create(@Nonnull WorkflowContext context) {
        if (workflowContextType.isAssignableFrom(context.getClass())) {
            return ((AbstractDSLWorkflowContext) context).execution();
        }

        throw new IllegalStateException("Unsupported context type " + context.getClass().getName());
    }
}
