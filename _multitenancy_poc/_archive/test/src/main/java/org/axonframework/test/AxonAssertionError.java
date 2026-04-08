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

package org.axonframework.test;

import java.util.Arrays;
import java.util.Objects;

/**
 * Error indication that an Assertion failed during a test case. The message of the error contains detailed information
 * about the failed assertion.
 *
 * @author Allard Buijze
 * @since 0.6
 */
public class AxonAssertionError extends AssertionError {

    private static final long serialVersionUID = 3731933425096971345L;

    /**
     * Create a new error instance using the given {@code detailMessage}.
     *
     * @param detailMessage A detailed description of the failed assertion.
     */
    public AxonAssertionError(String detailMessage) {
        super(detailMessage);
        StackTraceElement[] original = getStackTrace();
        setStackTrace(cleanStackTrace(original));
    }

    /**
     * Create a new error instance using the given {@code cause} and {@code detailMessage}.
     *
     * @param detailMessage A detailed description of the failed assertion.
     * @param cause         The cause of the error.
     */
    public AxonAssertionError(String detailMessage, Throwable cause) {
        super(Objects.requireNonNull(detailMessage), Objects.requireNonNull(cause));
        StackTraceElement[] original = getStackTrace();
        setStackTrace(cleanStackTrace(original));
    }

    private StackTraceElement[] cleanStackTrace(StackTraceElement[] original) {
        int ignoreCount = 0;
        for (StackTraceElement element : original) {
            if (element.getClassName().startsWith("org.axonframework.test")) {
                ignoreCount++;
            } else {
                break;
            }
        }
        return Arrays.copyOfRange(original, ignoreCount, original.length);
    }
}
