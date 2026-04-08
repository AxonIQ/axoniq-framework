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

package org.axonframework.messaging.eventsourcing;

import org.axonframework.common.AxonNonTransientException;

/**
 * Exception indicating that an aggregate was not compatible with the requirements of the {@link
 * GenericAggregateFactory}.
 *
 * @author Allard Buijze
 * @since 0.5
 */
public class IncompatibleAggregateException extends AxonNonTransientException {

    /**
     * Initialize the exception with given {@code message} and {@code cause}.
     *
     * @param message Message describing the reason the aggregate is not compatible
     * @param cause   The cause
     */
    public IncompatibleAggregateException(String message, Exception cause) {
        super(message, cause);
    }

    /**
     * Initialize the exception with given {@code message}.
     *
     * @param message Message describing the reason the aggregate is not compatible
     */
    public IncompatibleAggregateException(String message) {
        super(message);
    }
}
