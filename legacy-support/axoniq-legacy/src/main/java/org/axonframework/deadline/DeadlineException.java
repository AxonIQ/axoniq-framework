/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline;

import org.axonframework.common.AxonTransientException;

/**
 * Exception which occurs during deadline message processing.
 *
 * @author Milan Savic
 * @since 3.3
 */
public class DeadlineException extends AxonTransientException {

    /**
     * Initializes deadline exception with message and no cause.
     *
     * @param message message describing what went wrong
     */
    public DeadlineException(String message) {
        super(message);
    }

    /**
     * Initializes deadline exception with message and actual cause.
     *
     * @param message message describing what went wrong
     * @param cause   actual cause of exception
     */
    public DeadlineException(String message, Throwable cause) {
        super(message, cause);
    }
}
