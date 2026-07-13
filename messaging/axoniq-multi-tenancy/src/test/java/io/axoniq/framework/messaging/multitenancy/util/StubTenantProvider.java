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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

/**
 * Test stub for {@link TenantProvider}, mirroring the subscribe-and-replay semantics of the Axon Server backed
 * provider without requiring a running Axon Server. Known tenants are replayed to newly subscribed components, and
 * tenants added or removed later trigger the registration hooks on all subscribed components. Cancelling a
 * subscription cancels every tenant registration made on the component's behalf, including registrations for tenants
 * added after subscribing, as the {@link TenantProvider#subscribe(MultiTenantAwareComponent)} contract requires.
 */
public class StubTenantProvider implements TenantProvider {

    private final List<TenantDescriptor> tenantDescriptors = new CopyOnWriteArrayList<>();
    private final List<MultiTenantAwareComponent> subscribedComponents = new CopyOnWriteArrayList<>();
    // One entry per tenant-component pair, so cancellation can select by either dimension.
    private final List<TenantRegistration> registrations = new CopyOnWriteArrayList<>();

    @Override
    public Registration subscribe(MultiTenantAwareComponent component) {
        subscribedComponents.add(component);
        tenantDescriptors.forEach(tenant -> registrations.add(new TenantRegistration(
                tenant, component, component.registerTenant(tenant)
        )));
        return () -> {
            subscribedComponents.remove(component);
            cancelRegistrationsMatching(registration -> registration.component() == component);
            return true;
        };
    }

    @Override
    public List<TenantDescriptor> tenants() {
        return List.copyOf(tenantDescriptors);
    }

    public void addTenant(TenantDescriptor tenant) {
        tenantDescriptors.add(tenant);
        subscribedComponents.forEach(component -> registrations.add(new TenantRegistration(
                tenant, component, component.registerAndStartTenant(tenant)
        )));
    }

    public void removeTenant(TenantDescriptor tenant) {
        if (tenantDescriptors.remove(tenant)) {
            cancelRegistrationsMatching(registration -> registration.tenant().equals(tenant));
        }
    }

    /**
     * Mirrors the Axon Server provider's shutdown: cancels every registration of every subscribed component, which is
     * what destroys the tenants' component instances at application shutdown.
     */
    public void shutdown() {
        cancelRegistrationsMatching(registration -> true);
    }

    public List<MultiTenantAwareComponent> subscribedComponents() {
        return List.copyOf(subscribedComponents);
    }

    // Synchronized like the Axon Server provider, so overlapping cancel paths never cancel the same entry twice.
    // Cancellation failures propagate on purpose: in a test they should fail the test, not be swallowed.
    private synchronized void cancelRegistrationsMatching(Predicate<TenantRegistration> criterion) {
        List<TenantRegistration> matching = registrations.stream().filter(criterion).toList();
        registrations.removeAll(matching);
        matching.reversed().forEach(tenantRegistration -> tenantRegistration.registration().cancel());
    }

    private record TenantRegistration(TenantDescriptor tenant,
                                      MultiTenantAwareComponent component,
                                      Registration registration) {

    }
}
