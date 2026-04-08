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
 * LockFactory implementation that does nothing. Can be useful in cases where a Locking Repository implementation needs
 * to be configured to ignore locks, for example in scenario's where an underlying storage mechanism already performs
 * the necessary locking.
 *
 * @author Allard Buijze
 * @since 0.6
 */
public enum NullLockFactory implements LockFactory {

    /**
     * Singleton instance of a {@link NullLockFactory}.
     */
    INSTANCE;

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation does nothing.
     */
    @Override
    public Lock obtainLock(String identifier) {
        return NoOpLock.INSTANCE;
    }
}
