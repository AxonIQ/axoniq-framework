/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.eventhandling.scheduling;

import org.axonframework.common.AxonTransientException;

import java.io.Serial;

/**
 * Exception indicating a problem in the Event Scheduling mechanism.
 *
 * @author Allard Buijze
 * @since 0.7
 */
public class SchedulingException extends AxonTransientException {

    @Serial
    private static final long serialVersionUID = -3633716643792480973L;

    /**
     * Initialize a SchedulingException with the given {@code message}.
     *
     * @param message the message describing the exception
     */
    public SchedulingException(String message) {
        super(message);
    }

    /**
     * Initialize a SchedulingException with the given {@code message} and {@code cause}.
     *
     * @param message the message describing the exception
     * @param cause   the cause of this exception
     */
    public SchedulingException(String message, Throwable cause) {
        super(message, cause);
    }
}
