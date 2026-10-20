/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.modelling.saga;

import org.axonframework.common.AxonException;

/**
 * Exception triggered by a saga while it is processing an event or processing a task.
 */
public class SagaExecutionException extends AxonException {

    /**
     * Initializes the exception using the given {@code message}.
     *
     * @param message the message describing the exception
     */
    public SagaExecutionException(String message) {
        super(message);
    }

    /**
     * Initializes the exception using the given {@code message} and {@code cause}.
     *
     * @param message The message describing the exception
     * @param cause   The underlying cause of the exception
     */
    public SagaExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
