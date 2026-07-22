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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;

import java.util.Objects;

/**
 * Primitive for programmatically terminating a workflow (cancellation or failure) or cancelling a single running step.
 * <p>
 * Each intent is carried by its own typed command record, so callers and implementations always know what is being
 * terminated from the type alone.
 *
 * @author Stefan Dragisic
 * @since 0.3.0
 */
public interface TerminatePrimitive {

    /**
     * Cancels the entire workflow, publishing the terminal {@code <workflow>:CANCELLED} event.
     *
     * @param command cancellation command.
     */
    void cancelWorkflow(@Nonnull CancelWorkflow command);

    /**
     * Fails the entire workflow, publishing the terminal {@code <workflow>:FAILED} event.
     *
     * @param command failure command.
     */
    void failWorkflow(@Nonnull FailWorkflow command);

    /**
     * Cancels a single running step, publishing its {@code <step>:CANCELLED} event while the workflow itself stays
     * non-terminal. The {@code <step>:CANCELLED} record is authored directly on the control thread (guarded on the
     * step still being non-terminal) before the step's future is torn down.
     *
     * @param command step cancellation command.
     * @return {@code true} if the step was non-terminal and a {@code <step>:CANCELLED} record was published;
     * {@code false} if the step was unknown or already terminal (nothing published).
     */
    boolean cancelStep(@Nonnull CancelStep command);

    /**
     * Command carrying the intent to cancel an entire workflow.
     *
     * @param cause                optional cancellation cause, or {@code null} if none.
     * @param eventNameCustomizer  customizer for the published event names.
     * @param workflowNameOverride optional override for the workflow name used on the published event, or {@code null}
     *                             to use the execution's own name.
     * @author Stefan Dragisic
     * @since 0.3.0
     */
    @Internal
    record CancelWorkflow(
            @Nullable Throwable cause,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nullable String workflowNameOverride
    ) {

        /**
         * Compact constructor validating the mandatory event name customizer.
         */
        public CancelWorkflow {
            Objects.requireNonNull(eventNameCustomizer, "EventNameCustomizer is required");
        }
    }

    /**
     * Command carrying the intent to fail an entire workflow.
     *
     * @param cause                optional failure cause, or {@code null} if none.
     * @param eventNameCustomizer  customizer for the published event names.
     * @param workflowNameOverride optional override for the workflow name used on the published event, or {@code null}
     *                             to use the execution's own name.
     * @author Stefan Dragisic
     * @since 0.3.0
     */
    @Internal
    record FailWorkflow(
            @Nullable Throwable cause,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nullable String workflowNameOverride
    ) {

        /**
         * Compact constructor validating the mandatory event name customizer.
         */
        public FailWorkflow {
            Objects.requireNonNull(eventNameCustomizer, "EventNameCustomizer is required");
        }
    }

    /**
     * Command carrying the intent to cancel a single running step.
     *
     * @param stepName            logical name of the step to cancel.
     * @param cause               optional cancellation cause, or {@code null} if none.
     * @param eventNameCustomizer customizer for the published event names.
     * @author Stefan Dragisic
     * @since 0.3.0
     */
    @Internal
    record CancelStep(
            @Nonnull String stepName,
            @Nullable Throwable cause,
            @Nonnull EventNameCustomizer eventNameCustomizer
    ) {

        /**
         * Compact constructor validating the mandatory step name and event name customizer.
         */
        public CancelStep {
            Objects.requireNonNull(stepName, "Step name is required");
            Objects.requireNonNull(eventNameCustomizer, "EventNameCustomizer is required");
        }
    }
}
