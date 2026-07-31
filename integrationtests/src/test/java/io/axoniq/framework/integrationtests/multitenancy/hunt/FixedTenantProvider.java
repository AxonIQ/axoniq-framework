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

package io.axoniq.framework.integrationtests.multitenancy.hunt;

import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import org.axonframework.common.Registration;

import java.util.ArrayList;
import java.util.List;

/**
 * In-memory {@link TenantProvider} with a fixed tenant set, for the in-memory differential arm: the same production
 * assembly minus Axon Server, mirroring the university example's demo provider.
 */
final class FixedTenantProvider implements TenantProvider {

    private final List<TenantDescriptor> tenants;

    FixedTenantProvider(String... tenantIds) {
        this.tenants = List.of(tenantIds).stream().map(TenantDescriptor::tenantWithId).toList();
    }

    @Override
    public List<TenantDescriptor> tenants() {
        return tenants;
    }

    @Override
    public boolean isKnown(TenantDescriptor tenant) {
        return tenants.stream().anyMatch(known -> known.tenantId().equals(tenant.tenantId()));
    }

    @Override
    public Registration subscribe(MultiTenantAwareComponent component) {
        List<Registration> registrations = new ArrayList<>();
        tenants.forEach(tenant -> registrations.add(component.registerTenant(tenant)));
        return () -> {
            registrations.forEach(Registration::cancel);
            return true;
        };
    }
}
