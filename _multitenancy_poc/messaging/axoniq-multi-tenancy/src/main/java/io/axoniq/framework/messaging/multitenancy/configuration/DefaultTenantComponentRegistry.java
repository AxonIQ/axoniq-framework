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

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.messaging.multitenancy.api.TenantComponentFactory;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentRegistry;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of the {@link TenantComponentRegistry}.
 * @param <T> the type of component this registry manages
 */
@Internal
public class DefaultTenantComponentRegistry<T> implements TenantComponentRegistry<T> {

    private final Class<T> componentType;
    private final TenantComponentFactory<T> factory;
    private final ConcurrentHashMap<TenantDescriptor, T> components = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TenantDescriptor, ConcurrentHashMap<Class<?>, T>> typedComponents =
            new ConcurrentHashMap<>();
    private final Set<TenantDescriptor> registeredTenants = ConcurrentHashMap.newKeySet();

    /**
     * Creates a new registry for the given component type and factory.
     *
     * @param componentType the class of the component type for parameter matching
     * @param factory       the factory to create component instances per tenant
     */
    public DefaultTenantComponentRegistry(Class<T> componentType,
                                          TenantComponentFactory<T> factory) {
        this.componentType = Objects.requireNonNull(componentType, "Component type must not be null");
        this.factory = Objects.requireNonNull(factory, "Factory must not be null");
    }

    @Override
    public T getComponent(TenantDescriptor tenant) {
        return components.computeIfAbsent(tenant, factory);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S extends T> S getComponent(TenantDescriptor tenant, Class<S> requestedType) {
        return (S) typedComponents
                .computeIfAbsent(tenant, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(requestedType, type -> factory.create(tenant, (Class<T>) type));
    }

    @Override
    public Class<T> getComponentType() {
        return componentType;
    }

    @Override
    public Set<TenantDescriptor> getTenants() {
        return Set.copyOf(registeredTenants);
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        registeredTenants.add(tenantDescriptor);
        // Lazy creation - don't create component until first access
        return () -> {
            boolean wasRegistered = registeredTenants.remove(tenantDescriptor);
            // Collect all unique component instances from both caches to avoid double-destroy
            Set<T> destroyed = Collections.newSetFromMap(new IdentityHashMap<>());
            T removed = components.remove(tenantDescriptor);
            if (removed != null) {
                destroyed.add(removed);
            }
            ConcurrentHashMap<Class<?>, T> typedForTenant = typedComponents.remove(tenantDescriptor);
            if (typedForTenant != null) {
                destroyed.addAll(typedForTenant.values());
            }
            destroyed.forEach(component -> factory.destroy(tenantDescriptor, component));
            return wasRegistered;
        };
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        // For component registries, there's nothing to "start"
        return registerTenant(tenantDescriptor);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tenants", registeredTenants);
    }
}
