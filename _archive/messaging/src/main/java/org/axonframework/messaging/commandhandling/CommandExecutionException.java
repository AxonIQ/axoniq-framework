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

package org.axonframework.messaging.commandhandling;


import org.axonframework.messaging.core.HandlerExecutionException;

/**
 * Indicates that an exception has occurred while handling a command. Typically, this class is used to wrap checked
 * exceptions that have been thrown from a Command Handler while processing an incoming command.
 * <p/>
 * By default, a stack trace is not generated for this exception. However, the stack trace creation can be enforced
 * explicitly via the constructor accepting the {@code writableStackTrace} parameter.
 *
 * @author Allard Buijze
 * @since 1.3
 */
public class CommandExecutionException extends HandlerExecutionException {

    /**
     * Initializes the exception with given {@code message} and {@code cause}.
     *
     * @param message The message describing the exception.
     * @param cause   The cause of the exception.
     */
    public CommandExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Initializes the exception with given {@code message}, {@code cause} and an object providing application-specific
     * {@code details}.
     *
     * @param message The message describing the exception.
     * @param cause   The cause of the exception.
     * @param details An object providing more error details (maybe {@code null}).
     */
    public CommandExecutionException(String message, Throwable cause, Object details) {
        super(message, cause, details);
    }

    /**
     * Initializes the exception with given {@code message}, {@code cause}, an object providing application-specific
     * {@code details}, and {@code writableStackTrace}
     *
     * @param message            The message describing the exception.
     * @param cause              The cause of the exception.
     * @param details            An object providing more error details (maybe {@code null}).
     * @param writableStackTrace Whether the stack trace should be generated ({@code true}) or not ({@code false}).
     */
    public CommandExecutionException(String message, Throwable cause, Object details, boolean writableStackTrace) {
        super(message, cause, details, writableStackTrace);
    }
}