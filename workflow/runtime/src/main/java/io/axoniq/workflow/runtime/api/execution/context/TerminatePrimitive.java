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
 * Primitive for programmatically terminating a workflow with either failure or cancellation.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public interface TerminatePrimitive {

    /**
     * Terminate a step or entire workflow.
     *
     * @param command termination command.
     */
    void terminate(@Nonnull TerminateCommand command);

    @Internal
    record TerminateCommand(
            boolean error,
            @Nullable Throwable cause,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nullable String workflowNameOverride,
            @Nullable String stepName
    ) {

        public TerminateCommand {
            Objects.requireNonNull(eventNameCustomizer, "EventNameCustomizer is required");
        }

        /**
         * Indicates whether this command targets a single step cancellation instead of whole-workflow termination.
         *
         * @return {@code true} when this command cancels a specific step, otherwise {@code false}
         */
        public boolean isStepCancellation() {
            return stepName != null;
        }

        /**
         * Creates a workflow cancellation command.
         *
         * @param cause optional cancellation cause
         * @param eventNameCustomizer customizer for published event names
         * @return termination command representing workflow cancellation
         */
        public static TerminateCommand cancel(@Nullable Throwable cause,
                                              @Nonnull EventNameCustomizer eventNameCustomizer) {
            return new TerminateCommand(false, cause, eventNameCustomizer, null, null);
        }

        /**
         * Creates a workflow failure command.
         *
         * @param cause optional failure cause
         * @param eventNameCustomizer customizer for published event names
         * @return termination command representing workflow failure
         */
        public static TerminateCommand fail(@Nullable Throwable cause,
                                            @Nonnull EventNameCustomizer eventNameCustomizer) {
            return new TerminateCommand(true, cause, eventNameCustomizer, null, null);
        }

        /**
         * Creates a cancellation command for a specific running step.
         *
         * @param stepName logical name of the step to cancel
         * @param cause optional cancellation cause
         * @param eventNameCustomizer customizer for published event names
         * @return termination command representing step cancellation
         */
        public static TerminateCommand cancelledStep(@Nonnull String stepName,
                                                     @Nullable Throwable cause,
                                                     @Nonnull EventNameCustomizer eventNameCustomizer) {
            return new TerminateCommand(false, cause, eventNameCustomizer, null, stepName);
        }
    }
}
