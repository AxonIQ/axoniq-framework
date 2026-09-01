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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory;

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
    public DSLAdoptingExecutionFactory(Class<C> workflowContextType) {
        this.workflowContextType = requireIsAssignableFrom(AbstractDSLWorkflowContext.class, workflowContextType);
    }

    @Override
    public WorkflowExecution create(WorkflowContext context) {
        if (workflowContextType.isAssignableFrom(context.getClass())) {
            return ((AbstractDSLWorkflowContext) context).execution();
        }

        throw new IllegalStateException("Unsupported context type " + context.getClass().getName());
    }
}
