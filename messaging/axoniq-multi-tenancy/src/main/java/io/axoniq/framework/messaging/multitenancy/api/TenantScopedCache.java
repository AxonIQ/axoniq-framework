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
import org.axonframework.common.infra.ComponentDescriptor;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A cache of one infrastructure component per tenant that creates each
 * component lazily on first use and evicts it when its tenant is removed.
 * <p>
 * As a {@link MultiTenantAwareComponent} it follows the tenant provider: {@link #registerTenant(TenantDescriptor)} and
 * {@link #registerAndStartTenant(TenantDescriptor)} return a {@link Registration} whose cancellation evicts the
 * tenant's cached component, so a re-added tenant rebuilds a fresh one. Component lifecycles beyond eviction (such as
 * the connection a component uses) are owned elsewhere.
 *
 * @param <S> the type of per-tenant component cached
 * @author Jakob Hatzl
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class TenantScopedCache<S> implements MultiTenantAwareComponent {

    private final Map<TenantDescriptor, S> components = new ConcurrentHashMap<>();
    private final Function<TenantDescriptor, S> componentFactory;

    /**
     * Constructs a {@code TenantScopedCache} building its components with the given {@code componentFactory}.
     *
     * @param componentFactory the factory building a tenant's component, invoked once per tenant on first access
     */
    public TenantScopedCache(Function<TenantDescriptor, S> componentFactory) {
        this.componentFactory = Objects.requireNonNull(componentFactory, "The component factory must not be null");
    }

    /**
     * Returns the component of the given {@code tenant}, creating and caching it on first access.
     *
     * @param tenant the tenant to return the component for
     * @return the tenant's cached component
     */
    public S componentFor(TenantDescriptor tenant) {
        return components.computeIfAbsent(tenant, componentFactory);
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        return () -> components.remove(tenantDescriptor) != null;
    }

    /**
     * Behaves identically to {@link #registerTenant(TenantDescriptor)}: components are created lazily on first use, so
     * there is nothing to start eagerly.
     */
    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        return registerTenant(tenantDescriptor);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tenants", components.keySet());
    }
}
