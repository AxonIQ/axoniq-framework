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
