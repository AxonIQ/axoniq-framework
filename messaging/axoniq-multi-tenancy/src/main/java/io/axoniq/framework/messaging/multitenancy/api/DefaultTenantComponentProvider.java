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

import java.util.List;
import java.util.Objects;

/**
 * Default implementation of {@link TenantComponentProvider}, caching one lazily created component instance per tenant.
 * <p>
 * A tenant becomes eligible for a component instance once it is {@link #registerTenant(TenantDescriptor) registered}.
 * The instance itself is only built on the first {@link #componentFor(TenantDescriptor) access} for that tenant.
 * Unregistering a tenant removes and {@link TenantComponentFactory#destroy(TenantDescriptor, Object) destroys} its
 * cached instance. Requesting the component of a tenant that is not registered is rejected with a
 * {@link TenantNotResolvedException}, so no instance is ever built for an unknown tenant.
 * <p>
 * Component creation runs inside the cache update. The factory's
 * {@link TenantComponentFactory#create(TenantDescriptor) create} method must therefore not register or unregister
 * tenants on, nor request components from, this provider on the creating thread.
 * <p>
 * Re-registering a tenant supersedes its previous registration, and cancelling a registration affects only the
 * instance created under it, so a stale cancellation never disturbs a newer registration of the same tenant. The
 * registration bookkeeping this rests on is {@link TenantScopedCache}, which this provider adds the component type and
 * the {@link TenantComponentFactory#destroy(TenantDescriptor, Object) destroy} half of the lifecycle to.
 * <p>
 * Internal, because users obtain this provider through
 * {@link TenantComponentProvider#withFactory(Class, TenantComponentFactory)} rather than constructing it directly.
 *
 * @param <T> the type of component provided per tenant
 * @author Theo Emanuelsson
 * @author Jan Galinski
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
class DefaultTenantComponentProvider<T> implements TenantComponentProvider<T> {

    private final Class<T> componentType;
    private final TenantScopedCache<T> instances;

    /**
     * Constructs a provider for the given {@code componentType}, using the given {@code factory} to build and destroy
     * the per-tenant instances.
     *
     * @param componentType the type of component provided per tenant, used to match against handler parameters
     * @param factory       the factory building and destroying the per-tenant instances
     * @throws NullPointerException if the given {@code componentType} or {@code factory} is {@code null}
     */
    DefaultTenantComponentProvider(Class<T> componentType,
                                   TenantComponentFactory<T> factory) {
        this.componentType = Objects.requireNonNull(componentType, "The component type must not be null");
        Objects.requireNonNull(factory, "The factory must not be null");
        this.instances = new TenantScopedCache<>(factory::create,
                                                 factory::destroy,
                                                 "the component provider for type [" + componentType.getName() + "]");
    }

    @Override
    public T componentFor(TenantDescriptor tenant) {
        return instances.componentFor(tenant);
    }

    @Override
    public Class<T> componentType() {
        return componentType;
    }

    @Override
    public List<TenantDescriptor> tenants() {
        return instances.tenants();
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        return instances.registerTenant(tenantDescriptor);
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        return instances.registerAndStartTenant(tenantDescriptor);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("componentType", componentType.getName());
        descriptor.describeProperty("tenants", instances.tenants());
    }
}
