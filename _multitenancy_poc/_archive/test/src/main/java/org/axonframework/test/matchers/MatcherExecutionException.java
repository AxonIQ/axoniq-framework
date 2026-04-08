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

package org.axonframework.test.matchers;

import org.axonframework.common.AxonNonTransientException;

/**
 * Exception indicating that an error occurred that prevented successful execution of a matcher. This exception does
 * not mean an actual instance does not match the expected instance. It means the matcher was unable to perform the
 * match due to external factors.
 *
 * @author Allard Buijze
 * @since 1.1
 */
public class MatcherExecutionException extends AxonNonTransientException {

    /**
     * Construct the exception with the given {@code message}.
     *
     * @param message the message describing the cause
     */
    public MatcherExecutionException(String message) {
        super(message);
    }

    /**
     * Construct the exception with the given {@code message} and {@code cause}.
     *
     * @param message the message describing the cause
     * @param cause   the underlying cause
     */
    public MatcherExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
