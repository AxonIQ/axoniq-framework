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

/**
 * Exception indicating an error has been caused that cannot be resolved without intervention. Retrying the operation
 * that threw the exception will most likely result in the same exception being thrown.
 * <p/>
 * Examples of such errors are programming errors and version conflicts.
 *
 * @author Allard Buijze
 * @since 0.6
 */
public abstract class AxonNonTransientException extends AxonException {

    /**
     * Indicates whether the given {@code throwable} is a AxonNonTransientException exception or indicates to be
     * caused by one.
     *
     * @param throwable The throwable to inspect
     * @return {@code true} if the given instance or one of it's causes is an instance of
     *         AxonNonTransientException, otherwise {@code false}
     */
    public static boolean isCauseOf(Throwable throwable) {
        return throwable != null
                && (throwable instanceof AxonNonTransientException || isCauseOf(throwable.getCause()));
    }

    /**
     * Initializes the exception using the given {@code message}.
     *
     * @param message The message describing the exception
     */
    public AxonNonTransientException(String message) {
        super(message);
    }

    /**
     * Initializes the exception using the given {@code message} and {@code cause}.
     *
     * @param message The message describing the exception
     * @param cause   The underlying cause of the exception
     */
    public AxonNonTransientException(String message, Throwable cause) {
        super(message, cause);
    }
}
