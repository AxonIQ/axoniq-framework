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

package org.axonframework.messaging.core.annotation;

import org.axonframework.common.AxonException;

/**
 * MessageHandlerInvocationException is a runtime exception that wraps an exception thrown by an invoked message
 * handler.
 *
 * @author Allard Buijze
 * @since 2.1
 */
public class MessageHandlerInvocationException extends AxonException {

    /**
     * Initialize the MessageHandlerInvocationException using given {@code message} and {@code cause}.
     *
     * @param message A message describing the cause of the exception
     * @param cause   The exception thrown by the Event Handler
     */
    public MessageHandlerInvocationException(String message, Throwable cause) {
        super(message, cause);
    }
}
