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
 * Exception signaling a {@link SequencedDeadLetterQueue} is overflowing.
 *
 * @author Steven van Beelen
 * @since 4.6.0
 */
public class DeadLetterQueueOverflowException extends AxonException {

    /**
     * Constructs an exception based on the given {@code message}.
     *
     * @param message The description of this {@link DeadLetterQueueOverflowException}.
     */
    public DeadLetterQueueOverflowException(String message) {
        super(message);
    }

    /**
     * Constructs an exception based on the given {@code identifier}.
     *
     * @param identifier The identifier referencing the sequence that has reached its limit.
     */
    public DeadLetterQueueOverflowException(Object identifier) {
        super("Unable to enqueue letter in sequence [" + identifier + "]. "
                      + "The maximum capacity of dead letters has been reached.");
    }
}
