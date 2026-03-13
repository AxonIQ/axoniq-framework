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
package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.engine.step.WorkflowStep;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Event sourced state of the workflow execution.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public interface WorkflowState extends DescribableComponent {

    /**
     * Retrieves a list of step names in the workflow execution.
     *
     * @return list of step names.
     */
    List<String> workflowStepNames();

    /**
     * Retrieves a step by name.
     *
     * @param stepName name of the step.
     * @return workflow step.
     */
    @Nonnull
    WorkflowStep getStep(@Nonnull String stepName);

    /**
     * Checks if a step with the given name exists in the workflow execution.
     *
     * @param stepName name of the step.
     * @return true if the step exists, false otherwise.
     */
    boolean containsStep(@Nonnull String stepName);

    /**
     * Returns the status of the workflow execution.
     *
     * @return workflow status.
     */
    @Nonnull
    WorkflowStatus workflowStatus();

    /**
     * Retrieves the payload of the workflow execution.
     *
     * @return payload of the workflow execution.
     */
    @Nonnull
    Map<String, Object> payload();

    /**
     * Guards against invoking any primitive when the workflow has already reached a terminal state. Rethrows the
     * original termination cause wrapped in the appropriate exception type.
     */
    void throwTerminalCause();

    /**
     * Handles an event message received during workflow execution. This handle is responsible for the modification of
     * the state.
     *
     * @param eventMessage      the event message received.
     * @param processingContext the processing context for the event.
     */
    void onEvent(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext);
}
