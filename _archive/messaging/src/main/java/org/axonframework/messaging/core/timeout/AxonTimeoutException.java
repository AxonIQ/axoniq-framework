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
package org.axonframework.messaging.core.timeout;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;

/**
 * Exception indicated that an Axon-specific task has timed out. This task can represent an entire
 * {@link UnitOfWork} or the handling of a specific
 * {@link Message}.
 *
 * @author Mitchell Herrijgers
 * @see AxonTimeLimitedTask
 * @see AxonTaskJanitor
 * @since 4.11.3
 */
public class AxonTimeoutException extends RuntimeException {

    /**
     * Initializes an {@link AxonTimeoutException} with the given {@code message}.
     *
     * @param message The message describing the cause of the timeout.
     */
    public AxonTimeoutException(String message) {
        super(message);
    }
}
