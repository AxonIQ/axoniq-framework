/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.axoniq.framework.messaging.multitenancy.api;

import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * Factory for creating and destroying tenant-scoped component instances.
 * <p>
 * Users implement this interface to define how components (such as repositories,
 * services, or any other dependencies) are created for each tenant. The factory
 * is invoked lazily when a component is first needed for a tenant.
 * <p>
 * When a tenant is removed, the {@link #destroy(TenantDescriptor, Object)} method
 * is called to release any resources held by the component. By default, this method
 * will close components that implement {@link AutoCloseable}.
 * <p>
 * Example usage:
 * <pre>{@code
 * TenantComponentFactory<CourseRepository> repoFactory =
 *     tenant -> new InMemoryCourseRepository();
 *
 * // Or with tenant-specific configuration:
 * TenantComponentFactory<CourseRepository> repoFactory =
 *     tenant -> new JpaCourseRepository(getDataSourceForTenant(tenant));
 *
 * // For components needing custom cleanup:
 * TenantComponentFactory<EntityManagerFactory> emfFactory =
 *     new TenantComponentFactory<>() {
 *         public EntityManagerFactory create(TenantDescriptor tenant) {
 *             return createEntityManagerFactory(tenant);
 *         }
 *         public void destroy(TenantDescriptor tenant, EntityManagerFactory emf) {
 *             emf.close();
 *             logger.info("Closed EMF for tenant {}", tenant.tenantId());
 *         }
 *     };
 * }</pre>
 *
 * @param <T> the type of component this factory creates
 * @author Theo Emanuelsson
 * @since 5.2.0
 * @see TenantComponentRegistry
 */
@FunctionalInterface
public interface TenantComponentFactory<T> extends Function<TenantDescriptor, T> {

    /**
     * Creates a component instance for the given tenant.
     *
     * @param tenant the tenant descriptor identifying which tenant needs the component
     * @return a new component instance for this tenant
     */
    T create(TenantDescriptor tenant);

    /**
     * Creates a component instance of the given {@code requestedType} for the given tenant.
     * <p>
     * This method is called when a handler parameter's type is a subtype of this factory's
     * {@link #componentType()}. For example, if this factory is registered for
     * {@code Repository.class}, and a handler declares an {@code OrderRepository} parameter,
     * this method is called with {@code requestedType = OrderRepository.class}.
     * <p>
     * The default implementation delegates to {@link #create(TenantDescriptor)}, which is
     * sufficient for factories that produce a single concrete type. Override this method
     * when the factory needs to create different instances based on the requested subtype.
     *
     * @param tenant        the tenant descriptor identifying which tenant needs the component
     * @param requestedType the specific subtype requested by the handler parameter
     * @param <S>          the subtype of component requested
     * @return a new component instance of the requested type for this tenant
     */
    @SuppressWarnings("unchecked")
    default <S extends T> S create(TenantDescriptor tenant, Class<S> requestedType) {
        return (S) create(tenant);
    }

    /**
     * Returns the type of component this factory creates, or {@code null} if the type
     * cannot be determined.
     * <p>
     * Implementations should override this when the generic type parameter {@code T} is
     * erased at runtime (e.g., when the factory is registered via a bean definition builder).
     * The default implementation returns {@code null}, which causes the framework to fall
     * back to generic type resolution via reflection.
     *
     * @return the component type, or {@code null}
     */
    // TODO: default null just to keep the functional interface? In case of generics - better TypeReference?
    default Class<T> componentType() {
        return null;
    }

    @Override
    default T apply(TenantDescriptor tenant) {
        return create(tenant);
    }

    /**
     * Destroys a component instance when a tenant is removed.
     * <p>
     * This method is called when a tenant is unregistered from the system. The default
     * implementation will close components that implement {@link AutoCloseable}. Override
     * this method to provide custom cleanup logic for components that require special
     * handling (e.g., releasing database connections, flushing caches, etc.).
     *
     * @param tenant    the tenant being removed
     * @param component the component instance to destroy
     */
    default void destroy(TenantDescriptor tenant, T component) {
        if (component instanceof AutoCloseable autoCloseable) {
            try {
                autoCloseable.close();
            } catch (Exception e) {
                LoggerFactory.getLogger(TenantComponentFactory.class)
                        .warn("Error closing AutoCloseable component for tenant [{}]: {}",
                                tenant.tenantId(), e.getMessage(), e);
            }
        }
    }
}