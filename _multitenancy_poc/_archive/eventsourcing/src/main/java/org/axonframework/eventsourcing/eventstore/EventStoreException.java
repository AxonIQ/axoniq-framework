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

package org.axonframework.eventsourcing.eventstore;

import org.axonframework.common.AxonNonTransientException;

/**
 * Indicates that the given events stream could not be stored or read due to an underlying exception.
 *
 * @author Allard Buijze
 * @since 0.1
 */
public class EventStoreException extends AxonNonTransientException {

    /**
     * Initialize the exception with the given {@code message}.
     *
     * @param message a detailed message of the cause of the exception
     */
    public EventStoreException(String message) {
        super(message);
    }

    /**
     * Initialize the exception with the given {@code message} and {@code cause}
     *
     * @param message a detailed message of the cause of the exception
     * @param cause   the original cause of this exception
     */
    public EventStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
