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

package io.axoniq.framework.messaging.multitenancy.util;

import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import org.axonframework.common.Registration;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test stub for {@link TenantProvider}, mirroring the subscribe-and-replay semantics of the Axon Server backed
 * provider without requiring a running Axon Server. Known tenants are replayed to newly subscribed components, and
 * tenants added or removed later trigger the registration hooks on all subscribed components.
 */
public class StubTenantProvider implements TenantProvider {

    private final List<TenantDescriptor> tenantDescriptors = new CopyOnWriteArrayList<>();
    private final List<MultiTenantAwareComponent> subscribedComponents = new CopyOnWriteArrayList<>();
    private final Map<TenantDescriptor, List<Registration>> registrationMap = new ConcurrentHashMap<>();

    @Override
    public Registration subscribe(MultiTenantAwareComponent component) {
        subscribedComponents.add(component);
        List<Registration> componentRegistrations = new CopyOnWriteArrayList<>();
        tenantDescriptors.forEach(tenant -> {
            Registration registration = component.registerTenant(tenant);
            registrationsFor(tenant).add(registration);
            componentRegistrations.add(registration);
        });
        return () -> {
            subscribedComponents.remove(component);
            componentRegistrations.forEach(Registration::cancel);
            registrationMap.values().forEach(registrations -> registrations.removeAll(componentRegistrations));
            return true;
        };
    }

    @Override
    public List<TenantDescriptor> tenants() {
        return List.copyOf(tenantDescriptors);
    }

    public void addTenant(TenantDescriptor tenant) {
        tenantDescriptors.add(tenant);
        subscribedComponents.forEach(
                component -> registrationsFor(tenant).add(component.registerAndStartTenant(tenant))
        );
    }

    public void removeTenant(TenantDescriptor tenant) {
        if (tenantDescriptors.remove(tenant)) {
            List<Registration> registrations = registrationMap.remove(tenant);
            if (registrations != null) {
                registrations.forEach(Registration::cancel);
            }
        }
    }

    /**
     * Mirrors the Axon Server provider's shutdown: cancels every registration of every subscribed component, which is
     * what destroys the tenants' component instances at application shutdown.
     */
    public void shutdown() {
        registrationMap.values().forEach(registrations -> registrations.forEach(Registration::cancel));
        registrationMap.clear();
    }

    public List<MultiTenantAwareComponent> subscribedComponents() {
        return List.copyOf(subscribedComponents);
    }

    private List<Registration> registrationsFor(TenantDescriptor tenant) {
        return registrationMap.computeIfAbsent(tenant, t -> new CopyOnWriteArrayList<>());
    }
}
