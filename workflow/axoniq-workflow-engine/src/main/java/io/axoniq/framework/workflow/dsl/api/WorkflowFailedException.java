/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;


/**
 * Exception thrown when a workflow fails.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
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
