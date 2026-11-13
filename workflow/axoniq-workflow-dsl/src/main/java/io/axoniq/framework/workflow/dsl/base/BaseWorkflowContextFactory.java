/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.base;

import org.jspecify.annotations.Nullable;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Map;

/**
 * Factory that creates {@link BaseWorkflowContext} instances for Java DSL
 * workflows.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class BaseWorkflowContextFactory implements WorkflowContextFactory<BaseWorkflowContext> {

    /**
     * Creates a workflow context for the current workflow invocation.
     *
     * @param initialPayload initial workflow payload
     * @param workflowId unique identifier of the workflow instance
     * @param processingContext processing context for the current message
     * @param workflowConfiguration runtime configuration for this workflow
     * @return workflow context passed to the Java DSL definition
     */
    @Override
    public BaseWorkflowContext createContext(
            Map<String, @Nullable Object> initialPayload,
            String workflowId,
            ProcessingContext processingContext,
            WorkflowConfiguration<?> workflowConfiguration
    ) {
        return new BaseWorkflowContext(
                workflowId,
                initialPayload,
                processingContext,
                workflowConfiguration
        );
    }
}
