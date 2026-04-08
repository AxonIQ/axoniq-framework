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

package org.axonframework.messaging.core;

import org.axonframework.common.AxonException;

/**
 * Exception indication that an error occurred while publishing an event to an AMQP Broker
 *
 * @author Allard Buijze
 * @author Nakul Mishra
 * @since 3.0
 */

public class EventPublicationFailedException extends AxonException {

    /**
     * Initialize the exception using given descriptive {@code message} and {@code cause}
     *
     * @param message A message describing the exception
     * @param cause   The exception describing the cause of the failure
     */
    public EventPublicationFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}