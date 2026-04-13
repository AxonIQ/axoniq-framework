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
 * Exception thrown when a workflow fails.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class WorkflowFailedException extends RuntimeException {

    /**
     * Constructs the exception.
     *
     * @param message message describing the error.
     */
    public WorkflowFailedException(@Nonnull String message) {
        super(message);
    }

    /**
     * Constructs the exception.
     *
     * @param message message describing the error.
     * @param cause   cause of the error.
     */
    public WorkflowFailedException(@Nonnull String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs the exception.
     *
     * @param cause cause of the error.
     */
    public WorkflowFailedException(@Nonnull Throwable cause) {
        super(cause);
    }
}
