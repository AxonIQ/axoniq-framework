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

package io.axoniq.framework.messaging.multitenancy.util;

import io.axoniq.framework.messaging.multitenancy.api.TenantChangeListener;
import io.axoniq.framework.messaging.multitenancy.api.TenantChangeSource;
import org.axonframework.common.Registration;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test stub for {@link TenantChangeSource}, announcing a tenant change exactly when the test says so.
 * <p>
 * Lets a test drive whatever follows a tenant change without standing up a tenant-routing infrastructure component, and
 * without going through a {@link io.axoniq.framework.messaging.multitenancy.api.TenantProvider TenantProvider}, which a
 * follower of this type does not observe.
 */
public class StubTenantChangeSource implements TenantChangeSource {

    private final CopyOnWriteArrayList<TenantChangeListener> listeners = new CopyOnWriteArrayList<>();

    @Override
    public Registration subscribe(TenantChangeListener listener) {
        listeners.addIfAbsent(listener);
        return () -> listeners.remove(listener);
    }

    /**
     * Returns how many listeners are currently subscribed, so a test can assert a subscription was cancelled.
     *
     * @return the number of currently subscribed listeners
     */
    public int listenerCount() {
        return listeners.size();
    }

    /**
     * Announces a tenant change to every subscribed listener, as a tenant-routing component does once a registration
     * took effect.
     */
    public void announceTenantsChanged() {
        listeners.forEach(TenantChangeListener::onTenantsChanged);
    }

}
