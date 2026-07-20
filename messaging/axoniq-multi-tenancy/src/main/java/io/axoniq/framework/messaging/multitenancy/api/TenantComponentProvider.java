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

/**
 * Provides the per-tenant instance of a single application component of type {@code T}.
 * <p>
 * A provider holds one instance of {@code T} per {@link TenantDescriptor tenant}, so message handlers always receive
 * the resource belonging to the tenant of the message being handled (for example a per-tenant SQL datasource or
 * repository). Not to be confused with the {@link TenantProvider}, which provisions the tenants themselves: a
 * {@code TenantComponentProvider} provides an application component scoped to each of those tenants.
 * <p>
 * A single provider is generic over <b>one</b> component type. To expose several tenant-scoped component types,
 * register a separate provider per type. Each provider is matched to handler parameters by its
 * {@link #componentType() component type}.
 * <p>
 * The default provider, obtained through {@link #withFactory(Class, TenantComponentFactory)}, creates instances
 * lazily through a {@link TenantComponentFactory} on first {@link #componentFor(TenantDescriptor) access} for a
 * tenant, and releases them again when that tenant is unregistered.
 * <p>
 * As a {@link MultiTenantAwareComponent} the provider participates in tenant lifecycle management. Registering a
 * tenant makes it eligible for component instances and unregistering it releases them.
 *
 * @param <T> the type of component provided per tenant
 * @author Theo Emanuelsson
 * @author Jan Galinski
 * @author Laura Devriendt
 * @see TenantComponentFactory
 * @since 5.3.0
 */
public interface TenantComponentProvider<T> extends MultiTenantAwareComponent, TenantDescriptors {

    /**
     * Creates a {@code TenantComponentProvider} for the given {@code componentType}, building and destroying the
     * per-tenant instances through the given {@code factory}.
     * <p>
     * The returned provider creates each tenant's instance lazily on first access and caches it. It only serves
     * {@link #registerTenant(TenantDescriptor) registered} tenants: requesting the component of an unknown tenant is
     * rejected with a {@link TenantNotResolvedException}, so no tenant-scoped resource is ever built for a tenant the
     * application does not know. Unregistering a tenant removes and
     * {@link TenantComponentFactory#destroy(TenantDescriptor, Object) destroys} its cached instance.
     * <p>
     * Component creation runs inside the provider's cache update. The given {@code factory}'s
     * {@link TenantComponentFactory#create(TenantDescriptor) create} method must therefore not register or unregister
     * tenants on, nor request components from, the returned provider on the creating thread.
     *
     * @param componentType the type of component provided per tenant, used to match against handler parameters, must
     *                      not be {@code null}
     * @param factory       the factory building and destroying the per-tenant instances, must not be {@code null}
     * @param <T>           the type of component provided per tenant
     * @return a provider serving one lazily created instance of {@code componentType} per registered tenant
     * @throws NullPointerException if the given {@code componentType} or {@code factory} is {@code null}
     */
    static <T> TenantComponentProvider<T> withFactory(Class<T> componentType, TenantComponentFactory<T> factory) {
        return new DefaultTenantComponentProvider<>(componentType, factory);
    }

    /**
     * Returns the component instance for the given {@code tenant}.
     * <p>
     * Implementations may reject tenants they do not serve with a {@link TenantNotResolvedException}. The provider
     * returned by {@link #withFactory(Class, TenantComponentFactory)} only serves registered tenants and creates each
     * tenant's instance lazily on first access.
     *
     * @param tenant the tenant to provide the component instance for
     * @return the component instance belonging to the given {@code tenant}
     * @throws TenantNotResolvedException if the given {@code tenant} is not served by this provider
     */
    T componentFor(TenantDescriptor tenant);

    /**
     * Returns the type of component provided per tenant.
     * <p>
     * Used to match this provider against message-handler parameters during parameter resolution. Matching is based
     * on the raw type: generic type parameters of the component type are not distinguished, so components differing
     * only in their generics need distinct wrapper types.
     *
     * @return the component type provided per tenant
     */
    Class<T> componentType();
}
