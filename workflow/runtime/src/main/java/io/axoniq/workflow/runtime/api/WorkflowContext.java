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

import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.List;
import java.util.Map;

/**
 * Public API facing class to access the execution from workflow definition.
 */
public interface WorkflowContext extends
        ExecutePrimitive,
        WaitForPrimitive,
        TerminatePrimitive,
        AllMatchCombinator,
        NoneMatchCombinator,
        AnyMatchCombinator,
        DescribableComponent {

    @Nonnull
    String workflowId();

    @Nonnull
    Map<String, Object> workflowPayload();

    void applyPayloadModification(@Nonnull PayloadModification payloadModification);

    @Nonnull
    WorkflowStatus workflowStatus();

    @Nonnull
    List<String> workflowStepNames();

    @Nonnull
    ProcessingContext processingContext();
}
