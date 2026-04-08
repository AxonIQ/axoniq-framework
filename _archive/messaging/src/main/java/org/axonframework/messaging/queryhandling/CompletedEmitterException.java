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

import org.axonframework.common.AxonNonTransientException;

/**
 * Exception indicating that {@link QueryUpdateEmitter} is completed, thus cannot be used to emit messages and report
 * errors.
 *
 * @author Milan Savic
 * @since 3.3
 */
public class CompletedEmitterException extends AxonNonTransientException {

    /**
     * Initializes the exception with given {@code message}.
     *
     * @param message the message
     */
    public CompletedEmitterException(String message) {
        super(message);
    }
}
