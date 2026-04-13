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

/**
 * Exception thrown when a workflow is cancelled via the terminate primitive.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class WorkflowCancelledException extends RuntimeException {

    /**
     * Constructs a {@code WorkflowCancelledException} with a descriptive message.
     *
     * @param message message describing the cancellation reason.
     */
    public WorkflowCancelledException(@Nonnull String message) {
        super(message);
    }

    /**
     * Constructs a {@code WorkflowCancelledException} with a descriptive message and a cause.
     *
     * @param message message describing the cancellation reason.
     * @param cause   cause of the cancellation.
     */
    public WorkflowCancelledException(@Nonnull String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a {@code WorkflowCancelledException} wrapping an underlying cause.
     *
     * @param cause cause of the cancellation.
     */
    public WorkflowCancelledException(@Nonnull Throwable cause) {
        super(cause);
    }
}
