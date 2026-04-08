/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.deadletter;

import org.axonframework.common.AxonException;

/**
 * Exception representing that a wrong dead letter was provided to the queue. All
 * {@link org.axonframework.messaging.deadletter.DeadLetter}s supplied back to the
 * {@link org.axonframework.messaging.deadletter.SequencedDeadLetterQueue}, for example the
 * {@link org.axonframework.messaging.deadletter.SequencedDeadLetterQueue#evict(DeadLetter,
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
