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
import io.axoniq.framework.messaging.multitenancy.api.WithTenantDescriptors;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;

import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CopyOnWriteArrayList;

public class RecordingTenantAwareComponent implements MultiTenantAwareComponent, WithTenantDescriptors {

    private final List<TenantDescriptor> registeredTenants = new CopyOnWriteArrayList<>();

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        registeredTenants.add(tenantDescriptor);
        return () -> registeredTenants.remove(tenantDescriptor);
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        return registerTenant(tenantDescriptor);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tenantCount", registeredTenants.size());
    }

    @Override
    public List<TenantDescriptor> tenants() {
        return List.copyOf(registeredTenants);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", RecordingTenantAwareComponent.class.getSimpleName() + "[", "]")
                .add("registeredTenants=" + registeredTenants)
                .toString();
    }
}
