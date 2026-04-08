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

package org.axonframework.messaging.eventhandling.scheduling;

import org.axonframework.common.AxonTransientException;

/**
 * Exception indicating a problem in the Event Scheduling mechanism.
 *
 * @author Allard Buijze
 * @since 0.7
 */
public class SchedulingException extends AxonTransientException {

    /**
     * Initialize a SchedulingException with the given {@code message}.
     *
     * @param message The message describing the exception
     */
    public SchedulingException(String message) {
        super(message);
    }

    /**
     * Initialize a SchedulingException with the given {@code message} and {@code cause}.
     *
     * @param message The message describing the exception
     * @param cause   The cause of this exception
     */
    public SchedulingException(String message, Throwable cause) {
        super(message, cause);
    }
}
