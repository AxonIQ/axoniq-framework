/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.util;

import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptors;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;

import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CopyOnWriteArrayList;

public class RecordingTenantAwareComponent implements MultiTenantAwareComponent, TenantDescriptors {

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
