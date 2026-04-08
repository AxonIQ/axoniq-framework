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
 * Exception indicating that a processor tried to claim a Token (either by retrieving or updating it) that has already
 * been claimed by another process. This typically happens when two processes (JVM's) contain processing with the same
 * name (and potentially the same configuration). In such case, only the first processor can use the TrackingToken.
 * <p>
 * Processes may retry obtaining the claim, preferably after a brief waiting period.
 */
public class UnableToClaimTokenException extends AxonTransientException {

    /**
     * Initialize the exception with given {@code message}.
     *
     * @param message The message describing the exception
     */
    public UnableToClaimTokenException(String message) {
        super(message);
    }

    /**
     * Initialize the exception with given {@code message} and {@code cause}.
     *
     * @param message The message describing the exception
     * @param cause   The cause of the failure
     */
    public UnableToClaimTokenException(String message, Throwable cause) {
        super(message, cause);
    }

}
