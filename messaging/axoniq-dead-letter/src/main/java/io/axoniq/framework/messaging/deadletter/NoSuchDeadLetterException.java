/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.deadletter;

import org.axonframework.common.AxonException;

/**
 * An {@link AxonException} describing that there is no such {@link DeadLetter dead letter} present in a
 * {@link SequencedDeadLetterQueue}.
 *
 * @author Steven van Beelen
 * @since 4.6.0
 */
public class NoSuchDeadLetterException extends AxonException {

    /**
     * Constructs an exception based on the given {@code message}.
     *
     * @param message The description of this {@link NoSuchDeadLetterException}.
     */
    public NoSuchDeadLetterException(String message) {
        super(message);
    }
}
