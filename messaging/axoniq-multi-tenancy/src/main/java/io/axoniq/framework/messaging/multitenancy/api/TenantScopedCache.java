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
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A cache of one infrastructure component per tenant that creates each component lazily on first use and evicts it when
 * its tenant is removed.
 * <p>
 * As a {@link MultiTenantAwareComponent} it follows the tenant provider: {@link #registerTenant(TenantDescriptor)} and
 * {@link #registerAndStartTenant(TenantDescriptor)} return a {@link Registration} whose cancellation evicts the
 * tenant's cached component, so a re-added tenant rebuilds a fresh one.
 * <p>
 * Eviction only drops this cache's reference to the component. That suits a component whose lifecycle is owned
 * elsewhere, such as one bound to a connection the connection manager closes. A component that has to be released on
 * eviction needs that release wired in, which {@link DefaultTenantComponentProvider} does to destroy the instances it
 * hands to message handlers. Such a release runs exactly once per component, whether its registration was cancelled,
 * superseded, or replaced while the component was being created.
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
    private final BiConsumer<TenantDescriptor, S> onEviction;
    private final String name;
    // Each registerTenant call is identified by its own token. Components are cached per token rather than per tenant,
    // so a cached component always traces back to the registration that created it.
    private final Map<TenantDescriptor, RegistrationToken> activeRegistrations = new ConcurrentHashMap<>();
    private final Map<RegistrationToken, S> components = new ConcurrentHashMap<>();
    // Refreshed on (un)registration, so a per-message tenants() call does not allocate on the hot path.
    private volatile List<TenantDescriptor> tenantsView = List.of();

    /**
     * Constructs a {@code TenantScopedCache} building its components with the given {@code componentFactory}, dropping
     * its reference to a component on eviction without releasing it any further.
     *
     * @param componentFactory the factory building a tenant's component, invoked once per tenant on first access
     * @throws NullPointerException if the given {@code componentFactory} is {@code null}
     */
    public TenantScopedCache(Function<TenantDescriptor, S> componentFactory) {
        this(componentFactory, TenantScopedCache::dropReference, "this cache");
    }

    /**
     * Constructs a {@code TenantScopedCache} building its components with the given {@code componentFactory} and
     * handing every component it evicts to the given {@code onEviction}.
     *
     * @param componentFactory the factory building a tenant's component, invoked once per tenant on first access
     * @param onEviction       invoked with a tenant and the component evicted for it, exactly once per evicted
     *                         component, so its owner can release it
     * @param name             how {@code this} cache refers to itself in the {@link TenantNotResolvedException} raised
     *                         for a tenant that is not registered
     * @throws NullPointerException if any of the given arguments is {@code null}
     */
    TenantScopedCache(Function<TenantDescriptor, S> componentFactory,
                      BiConsumer<TenantDescriptor, S> onEviction,
                      String name) {
        this.componentFactory = Objects.requireNonNull(componentFactory, "The component factory must not be null");
        this.onEviction = Objects.requireNonNull(onEviction, "The eviction callback must not be null");
        this.name = Objects.requireNonNull(name, "The name must not be null");
    }

    /**
     * Returns the component of the given {@code tenant}, creating and caching it on first access.
     *
     * @param tenant the tenant to return the component for
     * @return the tenant's cached component
     * @throws TenantNotResolvedException if the given {@code tenant} is not registered
     */
    public S componentFor(TenantDescriptor tenant) {
        Objects.requireNonNull(tenant, "The tenant must not be null");
        // Retried only when a concurrent (un)registration intervened between reading the token and checking it again,
        // so a caller progresses unless the tenant is registered anew without bound. Bounding the retries instead would
        // fail an operation that a single unlucky interleaving could have completed.
        while (true) {
            RegistrationToken token = activeRegistrations.get(tenant);
            if (token == null) {
                throw new TenantNotResolvedException("Tenant [%s] is not registered with %s",
                                                     tenant.tenantId(),
                                                     name);
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
            // the next iteration, a re-added one gets a component under its newer registration. Only the thread that
            // removes it evicts it, so the callback runs exactly once.
            if (components.remove(token, component)) {
                onEviction.accept(tenant, component);
            }
        }
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        Objects.requireNonNull(tenantDescriptor, "The tenant descriptor must not be null");
        RegistrationToken token = new RegistrationToken();
        // Re-registering supersedes the previous registration, whose component would otherwise be reachable through
        // neither this cache nor its own cancellation once that Registration is dropped.
        RegistrationToken superseded = activeRegistrations.put(tenantDescriptor, token);
        refreshTenantsView();
        evict(tenantDescriptor, superseded);
        return () -> {
            // The tenant-scoped remove only succeeds while this registration is still the active one, and the
            // token-scoped remove only yields the component created under it. Cancelling is therefore idempotent and
            // never evicts the component of a newer registration of the same tenant.
            boolean wasRegistered = activeRegistrations.remove(tenantDescriptor, token);
            if (wasRegistered) {
                refreshTenantsView();
            }
            evict(tenantDescriptor, token);
            return wasRegistered;
        };
    }

    /**
     * Behaves identically to {@link #registerTenant(TenantDescriptor)}: components are created lazily on first use, so
     * there is nothing to start eagerly.
     *
     * @param tenantDescriptor the tenant to register with {@code this} cache
     * @return a registration whose cancellation evicts the tenant's component
     */
    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        return registerTenant(tenantDescriptor);
    }

    /**
     * Returns the tenants currently registered with {@code this} cache, whether their component was built yet or not.
     *
     * @return the tenants currently registered with {@code this} cache
     */
    public List<TenantDescriptor> tenants() {
        return tenantsView;
    }

    private static <C> void dropReference(TenantDescriptor tenant, C component) {
        // The component's lifecycle is owned elsewhere, so eviction only drops this cache's reference to it.
    }

    private void evict(TenantDescriptor tenant, @Nullable RegistrationToken token) {
        if (token == null) {
            return;
        }
        S evicted = components.remove(token);
        if (evicted != null) {
            onEviction.accept(tenant, evicted);
        }
    }

    // Synchronized so concurrent (un)registrations cannot publish an older snapshot last, leaving the view stale.
    private synchronized void refreshTenantsView() {
        this.tenantsView = List.copyOf(activeRegistrations.keySet());
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("componentFactory", componentFactory);
        // Describes the immutable snapshot, so descriptors serialized lazily never observe mid-mutation state.
        descriptor.describeProperty("tenants", tenantsView);
    }

    // Identifies a single registerTenant call, tying tenant membership and component ownership to that registration.
    private static final class RegistrationToken {

    }
}
