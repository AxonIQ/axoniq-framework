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

import org.slf4j.LoggerFactory;

/**
 * Creates and destroys the per-tenant instances held by a {@link TenantComponentProvider}.
 * <p>
 * Implement this interface to define how a tenant-scoped component (such as a repository, service, or datasource) is
 * built for a given {@link TenantDescriptor tenant}. The provider invokes {@link #create(TenantDescriptor)} lazily,
 * the first time a component is needed for a tenant, and {@link #destroy(TenantDescriptor, Object)} when that tenant is
 * removed. The default {@code destroy} closes components that implement {@link AutoCloseable}. Override it for custom
 * cleanup.
 * <p>
 * Example usage:
 * <pre>{@code
 * // Build a tenant-specific repository from the tenant's datasource:
 * TenantComponentFactory<CourseRepository> factory =
 *         tenant -> new JdbcCourseRepository(dataSourceFor(tenant));
 * }</pre>
 *
 * @param <T> the type of component this factory creates
 * @author Theo Emanuelsson
 * @author Jan Galinski
 * @author Laura Devriendt
 * @see TenantComponentProvider
 * @since 5.3.0
 */
@FunctionalInterface
public interface TenantComponentFactory<T> {

    /**
     * Creates a component instance for the given {@code tenant}.
     *
     * @param tenant the tenant to create the component instance for
     * @return a new component instance for the given {@code tenant}
     */
    T create(TenantDescriptor tenant);

    /**
     * Destroys the given {@code component} when its {@code tenant} is removed.
     * <p>
     * The default implementation closes components that implement {@link AutoCloseable}. Failures to close are logged
     * and suppressed, so the remaining tenants' components are still cleaned up. Override this method to release other
     * tenant-scoped resources (for example database connections or caches).
     *
     * @param tenant    the tenant being removed
     * @param component the component instance to destroy
     */
    default void destroy(TenantDescriptor tenant, T component) {
        if (component instanceof AutoCloseable autoCloseable) {
            try {
                autoCloseable.close();
            } catch (Exception e) {
                // Looked up per invocation, since interface fields would be public constants.
                LoggerFactory.getLogger(TenantComponentFactory.class)
                             .warn("Error closing AutoCloseable component for tenant [{}]", tenant.tenantId(), e);
            }
        }
    }
}
