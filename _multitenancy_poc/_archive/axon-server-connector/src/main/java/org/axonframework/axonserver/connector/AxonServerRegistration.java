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

package org.axonframework.axonserver.connector;

import org.axonframework.common.Registration;

/**
 * Wrapper around standard Axon Framework {@link Registration}. Notifies messaging server when registration is cancelled
 * or closed, and delegates the close/cancel to the normal registration.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class AxonServerRegistration implements Registration {

    private final Registration wrappedRegistration;
    private final Runnable closeCallback;

    /**
     * Instantiate an {@link AxonServerRegistration}, which wraps the given {@code wrappedRegistration} and runs the
     * provided {@code closeCallback} on {@link #cancel()} call
     *
     * @param wrappedRegistration the {@link Registration} wrapped by this Axon Server specific registration
     * @param closeCallback       a {@link Runnable} executed on a {@link #cancel()} call
     */
    public AxonServerRegistration(Registration wrappedRegistration, Runnable closeCallback) {
        this.wrappedRegistration = wrappedRegistration;
        this.closeCallback = closeCallback;
    }

    @Override
    public boolean cancel() {
        boolean result = wrappedRegistration.cancel();
        if (result) {
            closeCallback.run();
        }
        return result;
    }
}
