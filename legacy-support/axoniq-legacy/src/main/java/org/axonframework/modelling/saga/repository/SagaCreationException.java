/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.modelling.saga.repository;

import org.axonframework.common.AxonNonTransientException;

/**
 * Exception triggered when a new saga instance could not be instantiated.
 */
public class SagaCreationException extends AxonNonTransientException {

    /**
     * Initializes the exception using the given {@code message} and {@code cause}.
     *
     * @param message The message describing the exception
     * @param cause   The underlying cause of the exception
     */
    public SagaCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}
