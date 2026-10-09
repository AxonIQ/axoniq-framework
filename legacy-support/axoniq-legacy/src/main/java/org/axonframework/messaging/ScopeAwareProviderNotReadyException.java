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

package org.axonframework.messaging;

import org.axonframework.common.AxonTransientException;

/**
 * Exception thrown by the {@link LegacyScopeAwareProvider} when a deadline fires before the configuration has started
 * its event processors, and the provider therefore cannot provide every component the deadline may be delivered to.
 * <p>
 * The exception is transient: the deadline manager that fired the deadline retries it later.
 *
 * @author Jakob Hatzl
 * @since 5.4.0
 */
public class ScopeAwareProviderNotReadyException extends AxonTransientException {

    /**
     * Initializes the exception with the given {@code message}.
     *
     * @param message the message describing why the provider is not ready
     */
    public ScopeAwareProviderNotReadyException(String message) {
        super(message);
    }

    /**
     * Initializes the exception with the given {@code message} and {@code cause}.
     *
     * @param message the message describing why the provider is not ready
     * @param cause   the cause of the provider not being ready
     */
    public ScopeAwareProviderNotReadyException(String message, Throwable cause) {
        super(message, cause);
    }
}
