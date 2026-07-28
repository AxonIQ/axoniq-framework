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
import java.util.Set;
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
 * <p>
 * Only a registered tenant gets a component. Requesting the component of a tenant that was never registered, or whose
 * registration was cancelled, is rejected with a {@link TenantNotResolvedException}. A removed tenant would otherwise
 * get a fresh component that nothing evicts again, since the registration that would have evicted it is already
 * cancelled, and for a backend-bound component that means holding a connection to a context that was just dropped.
 * <p>
 * Components are cached per registration rather than per tenant, so one created concurrently with the removal of its
 * tenant is discarded instead of outliving that registration.
 * <p>
 * Component creation runs inside the cache update. The factory must therefore not register or unregister tenants on,
 * nor request components from, this cache on the creating thread.
 *
 * @param <S> the type of per-tenant component cached
 * @author Jakob Hatzl
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class TenantScopedCache<S> implements MultiTenantAwareComponent {

    private final Function<TenantDescriptor, S> componentFactory;
    // Each registerTenant call is identified by its own token. Components are cached per token rather than per tenant,
    // so a cached component always traces back to the registration that created it.
    private final Map<TenantDescriptor, RegistrationToken> activeRegistrations = new ConcurrentHashMap<>();
    private final Map<RegistrationToken, S> components = new ConcurrentHashMap<>();

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
     * @throws TenantNotResolvedException if the given {@code tenant} is not registered with this cache
     */
    public S componentFor(TenantDescriptor tenant) {
        Objects.requireNonNull(tenant, "The tenant must not be null");
        while (true) {
            RegistrationToken token = activeRegistrations.get(tenant);
            if (token == null) {
                throw new TenantNotResolvedException(
                        "Tenant [%s] is not registered with this cache, so it has no component",
                        tenant.tenantId());
            }
            S component = components.computeIfAbsent(
                    token,
                    ignored -> Objects.requireNonNull(componentFactory.apply(tenant),
                                                      "The component factory returned null for tenant ["
                                                              + tenant.tenantId() + "]")
            );
            if (activeRegistrations.get(tenant) == token) {
                return component;
            }
            // The registration was cancelled or superseded while the component was being created, so the component
            // would escape the eviction of its registration. Drop it and re-evaluate: a removed tenant is rejected on
            // the next iteration, a re-added one gets a component under its newer registration.
            components.remove(token, component);
        }
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        Objects.requireNonNull(tenantDescriptor, "The tenant descriptor must not be null");
        RegistrationToken token = new RegistrationToken();
        // Re-registering supersedes the previous registration, whose component would otherwise be reachable through
        // neither this cache nor its own cancellation once that Registration is dropped.
        RegistrationToken superseded = activeRegistrations.put(tenantDescriptor, token);
        if (superseded != null) {
            components.remove(superseded);
        }
        return () -> {
            // The tenant-scoped remove only succeeds while this registration is still the active one, and the
            // token-scoped remove only yields the component created under it. Cancelling is therefore idempotent and
            // never evicts the component of a newer registration of the same tenant.
            boolean wasRegistered = activeRegistrations.remove(tenantDescriptor, token);
            components.remove(token);
            return wasRegistered;
        };
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
        // Describes an immutable snapshot, so descriptors serialized lazily never observe mid-mutation state.
        descriptor.describeProperty("tenants", Set.copyOf(activeRegistrations.keySet()));
    }

    // Identifies a single registerTenant call, tying tenant membership and component ownership to that registration.
    private static final class RegistrationToken {

    }
}
