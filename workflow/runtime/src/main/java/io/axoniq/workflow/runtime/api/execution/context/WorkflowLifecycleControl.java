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

/**
 * Internal control contract for programmatically changing workflow and step lifecycle state.
 * <p>
 * Each intent is carried by its own typed command, so callers and implementations know which lifecycle transition is
 * requested from its type alone.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public interface WorkflowLifecycleControl {

    /**
     * Cancels the entire workflow, publishing the terminal {@code <workflow>:CANCELLED} event.
     *
     * @param command cancellation command.
     */
    void cancelWorkflow(@Nonnull CancelWorkflowCommand command);

    /**
     * Fails the entire workflow, publishing the terminal {@code <workflow>:FAILED} event.
     *
     * @param command failure command.
     */
    void failWorkflow(@Nonnull FailWorkflowCommand command);

    /**
     * Cancels a single running step while the workflow itself stays non-terminal.
     * <p>
     * The control operation completes the step's running future exceptionally. The owning step executor subsequently
     * authors the guarded {@code <step>:CANCELLED} event through its normal terminal-event path.
     *
     * @param command step cancellation command.
     * @return {@code true} if cancellation was initiated for a running, non-terminal step, otherwise {@code false}
     */
    boolean cancelStep(@Nonnull CancelStepCommand command);

    /**
     * Command carrying the intent to cancel an entire workflow.
     *
     * @author Stefan Dragisic
     * @since 0.3.0
     */
    interface CancelWorkflowCommand {

        /**
         * Returns the optional cancellation cause.
         *
         * @return cancellation cause, or {@code null} when none was supplied
         */
        @Nullable
        Throwable cause();

        /**
         * Returns the customizer for published event names.
         *
         * @return event name customizer
         */
        @Nonnull
        EventNameCustomizer eventNameCustomizer();
    }

    /**
     * Command carrying the intent to fail an entire workflow.
     *
     * @author Stefan Dragisic
     * @since 0.3.0
     */
    interface FailWorkflowCommand {

        /**
         * Returns the optional failure cause.
         *
         * @return failure cause, or {@code null} when none was supplied
         */
        @Nullable
        Throwable cause();

        /**
         * Returns the customizer for published event names.
         *
         * @return event name customizer
         */
        @Nonnull
        EventNameCustomizer eventNameCustomizer();
    }

    /**
     * Command carrying the intent to cancel a single running step.
     *
     * @author Stefan Dragisic
     * @since 0.3.0
     */
    interface CancelStepCommand {

        /**
         * Returns the logical name of the step to cancel.
         *
         * @return step name
         */
        @Nonnull
        String stepName();

        /**
         * Returns the optional cancellation cause.
         *
         * @return cancellation cause, or {@code null} when none was supplied
         */
        @Nullable
        Throwable cause();

        /**
         * Returns the customizer for published event names.
         *
         * @return event name customizer
         */
        @Nonnull
        EventNameCustomizer eventNameCustomizer();
    }
}
