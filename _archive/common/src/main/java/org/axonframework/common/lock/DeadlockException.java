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

/**
 * Exception indicating that a deadlock has been detected while a thread was attempting to acquire a lock. This
 * typically happens when a Thread attempts to acquire a lock that is owned by a Thread that is in turn waiting for a
 * lock held by the current thread.
 * <p/>
 * It is typically safe to retry the operation when this exception occurs.
 *
 * @author Allard Buijze
 * @since 2.0
 */
public class DeadlockException extends LockAcquisitionFailedException {

    /**
     * Initializes the exception with given {@code message}.
     *
     * @param message The message describing the exception
     */
    public DeadlockException(String message) {
        super(message);
    }
}
