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

package org.axonframework.common.lock;

import org.axonframework.common.AxonTransientException;

/**
 * Exception indicating that a lock could not be obtained. Typically, operations failing with this exception can be
 * retried without any intervention.
 *
 * @author Allard Buijze
 * @since 2.0
 */
public class LockAcquisitionFailedException extends AxonTransientException {

    /**
     * Initialize the exception with given {@code message} and {@code cause}
     *
     * @param message The message describing the exception
     * @param cause   The underlying cause of the exception
     */
    public LockAcquisitionFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Initialize the exception with given {@code message}.
     *
     * @param message The message describing the exception
     */
    public LockAcquisitionFailedException(String message) {
        super(message);
    }
}
