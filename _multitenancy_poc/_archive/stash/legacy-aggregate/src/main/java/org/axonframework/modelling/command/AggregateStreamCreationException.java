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

package org.axonframework.modelling.command;

import org.axonframework.common.AxonException;

/**
 * Exception indicating that concurrent access to a repository was detected. Most likely, two threads were using the
 * same aggregate identifier to create an aggregate.
 *
 * @author Lucas Campos
 * @since 4.3
 */
public class AggregateStreamCreationException extends AxonException {

    /**
     * Initialize the exception with the given {@code message}.
     *
     * @param message a detailed message of the cause of the exception
     */
    public AggregateStreamCreationException(String message) {
        super(message);
    }

    /**
     * Initialize the exception with the given {@code message} and {@code cause}
     *
     * @param message a detailed message of the cause of the exception
     * @param cause   the original cause of this exception
     */
    public AggregateStreamCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}
