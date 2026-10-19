/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.messaging.multitenancy.api;

import org.axonframework.common.Registration;
import org.axonframework.common.infra.DescribableComponent;

/**
 * Interface for components that can be registered with a {@link TenantProvider}.
 *
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public interface MultiTenantAwareComponent extends DescribableComponent {

    /**
     * Registers the given {@code tenantDescriptor} as a known tenant with this multi-tenant aware component.
     * <p>
     * The caller must retain the returned {@link Registration} and cancel it when the tenant is removed, since
     * releasing the component's per-tenant resources rides on that cancellation.
     *
     * @param tenantDescriptor The {@link TenantDescriptor} to register with this component.
     * @return A {@link Registration} used to deregister the given {@code tenantDescriptor}.
     */
    Registration registerTenant(TenantDescriptor tenantDescriptor);

    /**
     * Registers the given {@code tenantDescriptor} as a known tenant with this multi-tenant aware component. If
     * applicable, this task will construct a tenant segment and start it.
     *
     * @param tenantDescriptor The {@link TenantDescriptor} to register with this component.
     * @return A {@link Registration} used to deregister the given {@code tenantDescriptor}.
     */
    Registration registerAndStartTenant(TenantDescriptor tenantDescriptor);
}
