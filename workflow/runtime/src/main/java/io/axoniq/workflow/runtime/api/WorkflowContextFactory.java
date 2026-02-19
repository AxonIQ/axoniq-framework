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

import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Map;

/**
 * Creates a context for workflow execution.
 *
 * @param <T> type of the context.
 */
@FunctionalInterface
public interface WorkflowContextFactory<T extends WorkflowContext> {

    @Nonnull
    T createContext(
            @Nonnull Map<String, Object> initialPayload,
            @Nonnull String workflowId,
            @Nonnull ProcessingContext processingContext,
            @Nonnull EventNameCustomizer eventNameCustomizer
    );
}
