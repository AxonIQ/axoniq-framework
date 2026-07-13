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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
 * instance created under it, so a stale cancellation never disturbs a newer registration of the same tenant.
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
    // Each registerTenant call is identified by its own token. Instances are cached per token rather than per
    // tenant, so membership and instance ownership always trace back to the registration that created them.
    private final ConcurrentMap<TenantDescriptor, RegistrationToken> activeRegistrations = new ConcurrentHashMap<>();
    private final ConcurrentMap<RegistrationToken, T> components = new ConcurrentHashMap<>();
    // Refreshed on (un)registration, so the per-message tenants() call does not allocate on the hot path.
    private volatile List<TenantDescriptor> tenantsView = List.of();

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
        this.factory = Objects.requireNonNull(factory, "The factory must not be null");
    }

    @Override
    public T componentFor(TenantDescriptor tenant) {
        Objects.requireNonNull(tenant, "The tenant must not be null");
        while (true) {
            RegistrationToken token = activeRegistrations.get(tenant);
            if (token == null) {
                throw unknownTenantException(tenant);
            }
            T component = components.computeIfAbsent(
                    token,
                    ignored -> Objects.requireNonNull(factory.create(tenant),
                                                      "The factory returned null for tenant [" + tenant.tenantId()
                                                              + "]")
            );
            if (activeRegistrations.get(tenant) == token) {
                return component;
            }
            // The registration was cancelled or superseded while creating, so the fresh instance would escape the
            // cleanup of its registration. Destroy it instead of leaking it and re-evaluate: a cancelled tenant is
            // rejected on the next iteration, a superseded one gets an instance under the newer registration.
            if (components.remove(token, component)) {
                factory.destroy(tenant, component);
            }
        }
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
        RegistrationToken token = new RegistrationToken();
        activeRegistrations.put(tenantDescriptor, token);
        refreshTenantsView();
        return () -> {
            // The tenant-scoped remove only succeeds while this registration is still the active one, and the
            // token-scoped remove only yields the instance created under it. Cancelling is therefore idempotent
            // and never affects a newer registration of the same tenant.
            boolean wasRegistered = activeRegistrations.remove(tenantDescriptor, token);
            if (wasRegistered) {
                refreshTenantsView();
            }
            T owned = components.remove(token);
            if (owned != null) {
                factory.destroy(tenantDescriptor, owned);
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
        this.tenantsView = List.copyOf(activeRegistrations.keySet());
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("componentType", componentType.getName());
        // Describes the immutable snapshot, so descriptors serialized lazily never observe mid-mutation state.
        descriptor.describeProperty("tenants", tenantsView);
    }

    // Identifies a single registerTenant call, tying tenant membership and instance ownership to that registration.
    private static final class RegistrationToken {

    }
}
