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
import jakarta.annotation.Nullable;

import java.util.Objects;

/**
 * Primitive for programmatically terminating a workflow with either failure or cancellation.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public interface TerminatePrimitive {

    void terminate(@Nonnull TerminateCommand command);

    void cancelStep(@Nonnull String stepName, @Nullable Throwable cause,
                    @Nonnull EventNameCustomizer eventNameCustomizer);

    record TerminateCommand(
            boolean error,
            @Nullable Throwable cause,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nullable String workflowNameOverride,
            @Nullable WorkflowConfiguration<?> configuration
    ) {
        public TerminateCommand {
            Objects.requireNonNull(eventNameCustomizer, "EventNameCustomizer is required");
        }

        public static TerminateCommand cancel(@Nonnull EventNameCustomizer eventNameCustomizer) {
            return new TerminateCommand(false, null, eventNameCustomizer, null, null);
        }

        public static TerminateCommand cancel(@Nullable Throwable cause,
                                              @Nonnull EventNameCustomizer eventNameCustomizer) {
            return new TerminateCommand(false, cause, eventNameCustomizer, null, null);
        }

        public static TerminateCommand fail(@Nullable Throwable cause,
                                            @Nonnull EventNameCustomizer eventNameCustomizer) {
            return new TerminateCommand(true, cause, eventNameCustomizer, null, null);
        }
    }
}
