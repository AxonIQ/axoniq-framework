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


/**
 * Exception thrown when a workflow fails.
 *
 * @author Simon Zambrovski
 * @since 0.1.0
 */
public class WorkflowFailedException extends RuntimeException {

    /**
     * Constructs the exception.
     *
     * @param message message describing the error.
     */
    public WorkflowFailedException(String message) {
        super(message);
    }

    /**
     * Constructs the exception.
     *
     * @param message message describing the error.
     * @param cause   cause of the error.
     */
    public WorkflowFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs the exception.
     *
     * @param cause cause of the error.
     */
    public WorkflowFailedException(Throwable cause) {
        super(cause);
    }
}
