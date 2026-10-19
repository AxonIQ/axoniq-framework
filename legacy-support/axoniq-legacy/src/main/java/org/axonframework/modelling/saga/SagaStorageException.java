/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.modelling.saga;

import org.axonframework.common.AxonTransientException;

/**
 * Exception indicating that an error has occurred while storing a Saga.
 *
 * @author Allard Buijze
 * @since 0.7
 */
public class SagaStorageException extends AxonTransientException {

    /**
     * Initialize a SagaStorageException with the given descriptive {@code message}.
     *
     * @param message The message describing the error.
     */
    public SagaStorageException(String message) {
        super(message);
    }

    /**
     * Initialize a SagaStorageException with the given descriptive {@code message} and {@code cause}.
     *
     * @param message The message describing the error.
     * @param cause   The cause of the error.
     */
    public SagaStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
