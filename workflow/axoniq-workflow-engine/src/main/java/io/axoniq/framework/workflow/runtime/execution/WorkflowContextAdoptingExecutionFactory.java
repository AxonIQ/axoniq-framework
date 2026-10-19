/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionFactory;

import static io.axoniq.framework.workflow.runtime.util.WorkflowReflectionUtils.requireIsAssignableFrom;

/**
 * Workflow execution factory adopting an author-facing context implementing {@link AbstractWorkflowContext}.
 *
 * @param <C> type of the implementation.
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class WorkflowContextAdoptingExecutionFactory<C extends WorkflowContext> implements
        WorkflowExecutionFactory {

    private final Class<C> workflowContextType;

    /**
     * Creates a new factory.
     *
     * @param workflowContextType type of the workflow context.
     */
    public WorkflowContextAdoptingExecutionFactory(Class<C> workflowContextType) {
        this.workflowContextType = requireIsAssignableFrom(AbstractWorkflowContext.class, workflowContextType);
    }

    @Override
    public WorkflowExecution create(WorkflowContext workflowContext) {
        if (workflowContextType.isInstance(workflowContext)) {
            return ((AbstractWorkflowContext) workflowContext).execution();
        }

        throw new IllegalStateException("Unsupported context type " + workflowContext.getClass().getName());
    }
}
