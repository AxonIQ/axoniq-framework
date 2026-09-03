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
package io.axoniq.workflow.dsl.base;

import org.jspecify.annotations.Nullable;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Map;

/**
 * Factory that creates {@link BaseWorkflowContext} instances for Java DSL
 * workflows.
 *
 * @author Simon Zambrovski
 * @since 0.1.0
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
