/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.deadletter;

import org.axonframework.common.AxonException;

/**
 * Exception representing that a wrong dead letter was provided to the queue. All
 * {@link io.axoniq.framework.messaging.deadletter.DeadLetter}s supplied back to the
 * {@link io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue}, for example the
 * {@link io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue#evict(DeadLetter,
 * org.axonframework.messaging.core.unitofwork.ProcessingContext)} method, should be the
 * original supplied by the queue in the first place.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class WrongDeadLetterTypeException extends AxonException {

    /**
     * Constructs a {@code WrongDeadLetterTypeException} with the provided {@code message}.
     *
     * @param message The message containing more details about the cause.
     */
    public WrongDeadLetterTypeException(String message) {
        super(message);
    }
}
