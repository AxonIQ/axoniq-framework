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
package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Map;

/**
 * A context factory is responsible for creation of the {@link SimpleWorkflowContext} instance passed into the worfkflow
 * method as a callback for all interaction with the workflow engine. The methods of the {@link SimpleWorkflowContext}
 * build a workflow DSL.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class SimpleWorkflowContextFactory implements WorkflowContextFactory<SimpleWorkflowContext> {

    @Nonnull
    @Override
    public SimpleWorkflowContext createContext(
            @Nonnull Map<String, Object> initialPayload,
            @Nonnull String workflowId,
            @Nonnull ProcessingContext processingContext,
            @Nonnull WorkflowConfiguration<?> workflowConfiguration
    ) {
        return new SimpleWorkflowContext(
                workflowId,
                initialPayload,
                processingContext,
                workflowConfiguration
        );
    }
}
