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
