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

package org.axonframework.modelling;

import org.axonframework.common.AxonTransientException;

/**
 * Exception indicating that concurrent access to a repository was detected. Most likely, two threads were modifying the
 * same entity.
 *
 * @author Allard Buijze
 * @since 0.3.0
 */
public class ConcurrencyException extends AxonTransientException {

    /**
     * Initialize a ConcurrencyException with the given {@code message}.
     *
     * @param message The message describing the cause of the exception.
     */
    public ConcurrencyException(String message) {
        super(message);
    }

    /**
     * Initialize a ConcurrencyException with the given {@code message} and {@code cause}.
     *
     * @param message The message describing the cause of the exception.
     * @param cause   The cause of the exception.
     */
    public ConcurrencyException(String message, Throwable cause) {
        super(message, cause);
    }
}
