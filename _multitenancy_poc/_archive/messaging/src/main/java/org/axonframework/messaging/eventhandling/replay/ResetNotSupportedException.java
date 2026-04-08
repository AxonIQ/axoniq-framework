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

package org.axonframework.messaging.eventhandling.replay;

import org.axonframework.common.AxonNonTransientException;

/**
 * Exception indicating that a reset is not supported by a component.
 *
 * @author Allard Buijze
 * @since 3.2
 */
public class ResetNotSupportedException extends AxonNonTransientException {

    /**
     * Initialize the exception with given {@code message}
     *
     * @param message a message describing the cause of the exception
     */
    public ResetNotSupportedException(String message) {
        super(message);
    }


    /**
     * Initialize the exception with given {@code message} and {@code cause}.
     *
     * @param message a message describing the cause of the exception
     * @param cause   the cause of this exception
     */
    public ResetNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }
}
