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
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;

import java.util.Map;

/**
 * Customizes workflow event names.
 */
public interface EventNameCustomizer {

    @Nonnull
    QualifiedName getEventName(@Nonnull String stepName, @Nonnull Map<String, Object> parameters,
                               @Nonnull StepStatus stepStatus);

    @Nonnull
    QualifiedName getEventName(@Nonnull String stepName, @Nonnull Map<String, Object> parameters,
                               @Nonnull WorkflowStatus stepStatus);

    @Nonnull
    EventNameCustomizer forStepInheritance();
}
