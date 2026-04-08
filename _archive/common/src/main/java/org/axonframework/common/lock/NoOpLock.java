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
 * No-op implementation of a {@link Lock}. Does nothing on {@link #release()} and returns {@code true} for
 * {@link #isHeld()}.
 *
 * @author Steven van Beelen
 * @since 4.5.11
 */
public class NoOpLock implements Lock {

    public static final Lock INSTANCE = new NoOpLock();

    private NoOpLock() {
        // Retrieve version through static INSTANCE.
    }

    @Override
    public void release() {
        // Not implemented for this no-op version.
    }

    @Override
    public boolean isHeld() {
        return true;
    }
}
