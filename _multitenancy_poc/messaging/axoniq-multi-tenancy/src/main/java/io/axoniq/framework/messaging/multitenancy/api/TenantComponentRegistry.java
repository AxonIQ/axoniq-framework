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

import java.util.Set;

/**
 * Registry that manages tenant-scoped component instances.
 * <p>
 * This registry caches component instances per tenant, creating them lazily on first access using the provided
 * {@link TenantComponentFactory}. When a tenant is unregistered, its component instance is removed from the cache and
 * cleaned up via {@link TenantComponentFactory#destroy(TenantDescriptor, Object)}.
 * <p>
 * The registry implements {@link MultiTenantAwareComponent} to participate in tenant lifecycle management, but uses
 * lazy creation - components are only instantiated when first requested, not when a tenant is registered.
 * <p>
 * Components that implement {@link AutoCloseable} are automatically closed when their tenant is removed (unless custom
 * cleanup is provided via the factory).
 *
 * @param <T> the type of component managed by this registry
 * @author Theo Emanuelsson
 * @see TenantComponentFactory
 * @since 5.3.0
 */
public interface TenantComponentRegistry<T> extends MultiTenantAwareComponent {

    /**
     * Gets the component instance for the given tenant, creating it if necessary.
     * <p>
     * Components are created lazily using the configured factory. Once created, they are cached for subsequent calls
     * with the same tenant.
     *
     * @param tenant the tenant descriptor
     * @return the component instance for this tenant
     */
    T getComponent(TenantDescriptor tenant);


    /**
     * Gets a component instance of the given {@code requestedType} for the given tenant.
     * <p>
     * This is used when a handler parameter's type is a subtype of this registry's component type. The factory's
     * {@link TenantComponentFactory#create(TenantDescriptor, Class)} method is called with the requested subtype.
     * Components are cached per (tenant, type) pair.
     *
     * @param tenant        the tenant descriptor
     * @param requestedType the specific subtype requested
     * @param <S>           the subtype
     * @return the component instance for this tenant and type
     */
    <S extends T> S getComponent(TenantDescriptor tenant, Class<S> requestedType);

    /**
     * Returns the component type managed by this registry.
     *
     * @return the component class
     */
    Class<T> getComponentType();

    /**
     * Returns the set of registered tenants.
     * <p>
     * Note that this returns all tenants that have been registered, not just those for which components have been
     * created. Use this for tenant resolution.
     *
     * @return an unmodifiable view of registered tenant descriptors
     */
    Set<TenantDescriptor> getTenants();
}
