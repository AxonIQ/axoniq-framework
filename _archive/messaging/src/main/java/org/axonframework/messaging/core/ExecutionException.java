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

package org.axonframework.messaging.core;

import org.axonframework.common.AxonTransientException;

/**
 * Exception thrown to indicate that execution of a task has failed. Depending on the cause of the execution exception
 * the operation could succeed if repeated.
 *
 * @author Rene de Waele
 * @since 3.0
 */
public class ExecutionException extends AxonTransientException {

    /**
     * Initialize an ExecutionException with the given {@code message} and {@code cause}.
     *
     * @param message The message describing the cause of the exception
     * @param cause   The cause of the exception
     */
    public ExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
