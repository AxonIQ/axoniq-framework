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
package io.axoniq.workflow.runtime.api.execution.context;

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Map;

/**
 * Creates a context for workflow execution.
 *
 * @param <T> type of the context.
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 0.1.0
 */
@FunctionalInterface
public interface WorkflowContextFactory<T extends WorkflowContext> {

    /**
     * Creates a new workflow context.
     *
     * @param initialPayload        initial payload of the workflow.
     * @param workflowId            id of the workflow.
     * @param processingContext     processing context.
     * @param workflowConfiguration workflow configuration.
     * @return workflow context.
     */
    T createContext(
            Map<String, @Nullable Object> initialPayload,
            String workflowId,
            ProcessingContext processingContext,
            WorkflowConfiguration<?> workflowConfiguration
    );
}
