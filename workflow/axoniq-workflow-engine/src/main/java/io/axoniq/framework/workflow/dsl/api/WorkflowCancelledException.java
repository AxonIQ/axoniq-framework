/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;


/**
 * Exception thrown when a workflow is cancelled via the terminate primitive.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class WorkflowCancelledException extends RuntimeException {

    /**
     * Constructs a {@code WorkflowCancelledException} with a descriptive message.
     *
     * @param message message describing the cancellation reason.
     */
    public WorkflowCancelledException(String message) {
        super(message);
    }

    /**
     * Constructs a {@code WorkflowCancelledException} with a descriptive message and a cause.
     *
     * @param message message describing the cancellation reason.
     * @param cause   cause of the cancellation.
     */
    public WorkflowCancelledException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a {@code WorkflowCancelledException} wrapping an underlying cause.
     *
     * @param cause cause of the cancellation.
     */
    public WorkflowCancelledException(Throwable cause) {
        super(cause);
    }
}
