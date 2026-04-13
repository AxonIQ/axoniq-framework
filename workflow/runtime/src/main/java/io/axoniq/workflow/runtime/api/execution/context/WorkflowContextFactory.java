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
package io.axoniq.workflow.runtime.api.execution.context;

import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Map;

/**
 * Creates a context for workflow execution.
 *
 * @param <T> type of the context.
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 1.0.0
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
    @Nonnull
    T createContext(
            @Nonnull Map<String, Object> initialPayload,
            @Nonnull String workflowId,
            @Nonnull ProcessingContext processingContext,
            @Nonnull WorkflowConfiguration<?> workflowConfiguration
    );
}
