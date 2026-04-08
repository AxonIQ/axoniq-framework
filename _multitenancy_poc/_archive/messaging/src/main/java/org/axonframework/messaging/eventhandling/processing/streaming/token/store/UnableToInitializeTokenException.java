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

package org.axonframework.messaging.eventhandling.processing.streaming.token.store;

import org.axonframework.common.AxonTransientException;

/**
 * Exception indicating that the TokenStore was unable to initialize a Token for a tracking processor and Segment.
 *
 * @author Allard Buijze
 * @since 4.1
 */
public class UnableToInitializeTokenException extends AxonTransientException {

    /**
     * Initialize the exception with given {@code message}
     *
     * @param message The message explaining the cause of the initialization problem
     */
    public UnableToInitializeTokenException(String message) {
        super(message);
    }

    /**
     * Initialize the exception with given {@code message} and underlying {@code cause}.
     *
     * @param message The message explaining the cause of the initialization problem
     * @param cause   The exception that cause the initialization to fail
     */
    public UnableToInitializeTokenException(String message, Throwable cause) {
        super(message, cause);
    }
}
