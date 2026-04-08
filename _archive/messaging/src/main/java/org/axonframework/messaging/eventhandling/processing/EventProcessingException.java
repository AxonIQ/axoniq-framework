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

package org.axonframework.messaging.eventhandling.processing;

import org.axonframework.common.AxonException;

/**
 * Exception thrown when an {@link EventProcessor} failed to handle a batch of events.
 *
 * @author Allard Buijze
 * @since 0.3
 */
public class EventProcessingException extends AxonException {

    /**
     * Initialize the exception with given {@code message} and {@code cause}.
     *
     * @param message Message describing the cause of the exception
     * @param cause   The exception that caused this exception to occur.
     */
    public EventProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
