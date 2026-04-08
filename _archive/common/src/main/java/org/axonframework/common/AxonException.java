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

package org.axonframework.common;

import org.jspecify.annotations.Nullable;

/**
 * Base exception for all Axon Framework related exceptions.
 *
 * @author Allard Buijze
 * @since 0.6
 */
public abstract class AxonException extends RuntimeException {

    /**
     * Initializes the exception using the given {@code message}.
     *
     * @param message The message describing the exception
     */
    public AxonException(String message) {
        super(message);
    }

    /**
     * Initializes the exception using the given {@code message} and {@code cause}.
     *
     * @param message The message describing the exception
     * @param cause   The underlying cause of the exception
     */
    public AxonException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    /**
     * Initializes the exception using the given {@code message}, {@code cause} and {@code writableStackTrace}.
     *
     * @param message            The message describing the exception
     * @param cause              The underlying cause of the exception
     * @param writableStackTrace Whether the stack trace should be generated ({@code true}) or not ({@code false})
     */
    public AxonException(String message, @Nullable Throwable cause, boolean writableStackTrace) {
        super(message, cause, true, writableStackTrace);
    }
}