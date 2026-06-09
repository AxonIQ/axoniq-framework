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
