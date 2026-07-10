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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * tenants on, nor request components from, this provider on the creating thread. Registering the same tenant more
 * than once is not supported: the resulting registrations are not distinguished, so cancelling any of them
 * unregisters the tenant.
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
    private final TenantComponentFactory<T> factory;
    private final ConcurrentMap<TenantDescriptor, T> components = new ConcurrentHashMap<>();
    private final Set<TenantDescriptor> registeredTenants = ConcurrentHashMap.newKeySet();
    // Refreshed on (un)registration, so the per-message tenants() call does not allocate on the hot path.
    private volatile List<TenantDescriptor> tenantsView = List.of();

    /**
     * Constructs a provider for the given {@code componentType}, using the given {@code factory} to build and destroy
     * the per-tenant instances.
     *
     * @param componentType the type of component provided per tenant, used to match against handler parameters
     * @param factory       the factory building and destroying the per-tenant instances
     */
    DefaultTenantComponentProvider(Class<T> componentType,
                                   TenantComponentFactory<T> factory) {
        this.componentType = Objects.requireNonNull(componentType, "The component type must not be null");
        this.factory = Objects.requireNonNull(factory, "The factory must not be null");
    }

    @Override
    public T componentFor(TenantDescriptor tenant) {
        Objects.requireNonNull(tenant, "The tenant must not be null");
        if (!registeredTenants.contains(tenant)) {
            throw unknownTenantException(tenant);
        }
        T component = components.computeIfAbsent(
                tenant,
                t -> Objects.requireNonNull(factory.create(t),
                                            "The factory returned null for tenant [" + t.tenantId() + "]")
        );
        // The tenant may have been unregistered concurrently, in which case the fresh instance would escape the
        // cleanup that ran during unregistration. Re-validate and destroy the instance instead of leaking it. The
        // value-checked remove confines the cleanup to the instance this thread created. A re-registration racing
        // this window may still observe the rejection, which is acceptable: the next access simply recreates.
        if (!registeredTenants.contains(tenant)) {
            if (components.remove(tenant, component)) {
                factory.destroy(tenant, component);
            }
            throw unknownTenantException(tenant);
        }
        return component;
    }

    private TenantNotResolvedException unknownTenantException(TenantDescriptor tenant) {
        return new TenantNotResolvedException(
                "Tenant [%s] is not registered with the component provider for type [%s]",
                tenant.tenantId(), componentType.getName()
        );
    }

    @Override
    public Class<T> componentType() {
        return componentType;
    }

    @Override
    public List<TenantDescriptor> tenants() {
        return tenantsView;
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        Objects.requireNonNull(tenantDescriptor, "The tenant descriptor must not be null");
        registeredTenants.add(tenantDescriptor);
        refreshTenantsView();
        AtomicBoolean cancelled = new AtomicBoolean(false);
        return () -> {
            // Cancelling is idempotent: a second cancel of a stale registration must not unregister a tenant
            // that was re-registered in the meantime, nor destroy its live component instance.
            if (!cancelled.compareAndSet(false, true)) {
                return false;
            }
            boolean wasRegistered = registeredTenants.remove(tenantDescriptor);
            refreshTenantsView();
            T removed = components.remove(tenantDescriptor);
            if (removed != null) {
                factory.destroy(tenantDescriptor, removed);
            }
            return wasRegistered;
        };
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        // Components are created lazily on first access, so there is nothing to start eagerly.
        return registerTenant(tenantDescriptor);
    }

    // Synchronized so concurrent (un)registrations cannot publish an older snapshot last, leaving the view stale.
    private synchronized void refreshTenantsView() {
        this.tenantsView = List.copyOf(registeredTenants);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("componentType", componentType.getName());
        // The immutable snapshot, so lazily serializing descriptors never observe mid-mutation state.
        descriptor.describeProperty("tenants", tenantsView);
    }
}
