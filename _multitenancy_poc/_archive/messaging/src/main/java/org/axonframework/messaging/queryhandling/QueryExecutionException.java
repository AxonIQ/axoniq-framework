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
package org.axonframework.messaging.queryhandling;

import org.axonframework.messaging.core.HandlerExecutionException;
import org.jspecify.annotations.Nullable;

/**
 * Exception indicating that the execution of a Query Handler has resulted in an exception.
 * <p/>
 * By default, a stack trace is not generated for this exception. However, the stack trace creation can be enforced explicitly
 * via the constructor accepting the {@code writableStackTrace} parameter.
 *
 * @author Marc Gathier
 * @since 3.1
 */
public class QueryExecutionException extends HandlerExecutionException {

    /**
     * Initializes the exception with given {@code message} and {@code cause}
     *
     * @param message Message explaining the context of the error
     * @param cause   The underlying cause of the invocation failure
     */
    public QueryExecutionException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    /**
     * Initializes the exception with given {@code message}, {@code cause} and {@code details}.
     *
     * @param message Message explaining the context of the error
     * @param cause   The underlying cause of the invocation failure
     * @param details An object providing more error details (may be {@code null})
     */
    public QueryExecutionException(String message, Throwable cause, Object details) {
        super(message, cause, details);
    }

    /**
     * Initializes the exception with given {@code message}, {@code cause}, {@code details} and
     * {@code writableStackTrace}
     *
     * @param message            Message explaining the context of the error
     * @param cause              The underlying cause of the invocation failure
     * @param details            An object providing more error details (may be {@code null})
     * @param writableStackTrace Whether the stack trace should be generated ({@code true}) or not ({@code false})
     */
    public QueryExecutionException(String message, Throwable cause, Object details, boolean writableStackTrace) {
        super(message, cause, details, writableStackTrace);
    }
}