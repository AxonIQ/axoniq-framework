/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package org.axonframework.deadline;

import org.axonframework.common.AxonTransientException;

/**
 * Exception which occurs during deadline message processing.
 *
 * @author Milan Savic
 * @since 3.3
 */
public class DeadlineException extends AxonTransientException {

    /**
     * Initializes deadline exception with message and no cause.
     *
     * @param message message describing what went wrong
     */
    public DeadlineException(String message) {
        super(message);
    }

    /**
     * Initializes deadline exception with message and actual cause.
     *
     * @param message message describing what went wrong
     * @param cause   actual cause of exception
     */
    public DeadlineException(String message, Throwable cause) {
        super(message, cause);
    }
}
