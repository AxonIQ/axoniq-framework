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

import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;

import java.util.Map;

/**
 * Customizes workflow event names.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public interface EventNameCustomizer {

    /**
     * Returns a customized event name based on the provided step name, parameters, and step status.
     *
     * @param stepName   the name of the step
     * @param parameters the parameters associated with the step
     * @param stepStatus the status of the step
     * @return the customized event name.
     */
    @Nonnull
    QualifiedName getEventName(@Nonnull String stepName, @Nonnull Map<String, Object> parameters,
                               @Nonnull StepStatus stepStatus);

    /**
     * Returns a customized event name based on the provided step name, parameters, and workflow status.
     *
     * @param stepName   the name of the step
     * @param parameters the parameters associated with the step
     * @param stepStatus the status of the step
     * @return the customized event name.
     */
    @Nonnull
    QualifiedName getEventName(@Nonnull String stepName, @Nonnull Map<String, Object> parameters,
                               @Nonnull WorkflowStatus stepStatus);

    /**
     * Returns a customized event name based on the provided step name, parameters, and workflow status.
     *
     * @return event name customizer that will use the namespace of the parent step.
     */
    @Nonnull
    EventNameCustomizer forStepInheritance();
}
