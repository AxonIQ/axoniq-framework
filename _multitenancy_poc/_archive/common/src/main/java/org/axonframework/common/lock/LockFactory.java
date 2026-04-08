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
 * Interface to the lock factory. A lock factory produces locks on resources that are shared between threads.
 *
 * @author Allard Buijze
 * @since 0.3
 */
@FunctionalInterface
public interface LockFactory {

    /**
     * Obtain a lock for a resource identified by given {@code identifier}. Depending on the strategy, this
     * method may return immediately or block until a lock is held.
     *
     * @param identifier the identifier of the resource to obtain a lock for.
     * @return a handle to release the lock.
     */
    Lock obtainLock(String identifier);
}
