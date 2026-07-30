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
package io.axoniq.framework.messaging.multitenancy.api;

import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;

/**
 * A source of changes to the set of tenants a component holds.
 * <p>
 * Implemented by a tenant-routing infrastructure component, for a component that has to act on that component's
 * tenants rather than on the tenants the {@link TenantProvider} knows. The provider notifies its subscribers in turn, so
 * one of them acting on a tenant change can observe a routing component that has not registered the tenant yet. A
 * subscriber of {@code this} source cannot: a change is announced only once it is visible through the announcing
 * component.
 * <p>
 * Registered as a component under {@code this} type, next to the type the implementation serves, so a follower has a
 * name to resolve that decorators of the served type do not match.
 * <p>
 * Internal, because the components implementing and following it are internal themselves.
 *
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public interface TenantChangeSource {

    /**
     * Subscribes the given {@code listener} to the changes of the tenants {@code this} source holds.
     * <p>
     * A listener is held once, compared by identity, so subscribing the same instance twice does not invoke it
     * twice for one change.
     *
     * @param listener the listener to invoke after every change to the tenants {@code this} source holds
     * @return a registration whose cancellation stops the given {@code listener} from being invoked further
     */
    Registration subscribe(TenantChangeListener listener);
}
