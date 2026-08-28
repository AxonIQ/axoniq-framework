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

import java.util.List;

/**
 * Contract towards a component that provisions the registered set of {@link TenantDescriptor tenants} and
 * {@link MultiTenantAwareComponent MultiTenantAwareComponents}.
 * <p>
 * Depending on the implementation the provider can monitor tenant changes and update the
 * {@code MultiTenantAwareComponents} accordingly.
 *
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public interface TenantProvider extends TenantDescriptors {

    /**
     * Subscribes the given {@code component} with this provider.
     * <p>
     * Tenants known to this provider are replayed to the component on subscription, and tenants added or removed
     * afterwards reach the component through its registration hooks. Cancelling the returned {@link Registration}
     * unsubscribes the component and cancels the tenant registrations made on its behalf, giving the component the
     * opportunity to release its per-tenant resources.
     *
     * @param component to be subscribed {@link MultiTenantAwareComponent} for tenant changes.
     * @return the registration for the given component.
     */
    Registration subscribe(MultiTenantAwareComponent component);

    /**
     * Get the list of registered {@link TenantDescriptor tenants} with this provided.
     *
     * @return The list of registered {@link TenantDescriptor tenants}.
     */
    @Override
    List<TenantDescriptor> tenants();
}
