/*
 * Copyright (c) 2010-2026. Axoniq B.V.
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
 * <p>
 * Implementations must keep nothing of a tenant whose registration throws: release whatever was allocated for it
 * before rethrowing, since the caller only holds the registrations that were returned. Releasing is best effort: if it
 * fails, attach that failure to the original one with {@code addSuppressed} and rethrow the original.
 * <p>
 * The provider's own undo is best effort as well: a cancel that itself fails can leave a resource behind. The provider
 * logs it and attaches it to the original failure as suppressed.
 * <p>
 * A returned registration may be cancelled right after it was returned, when another component fails to register the
 * same tenant and the tenant provider rolls the registration back. Implementers must therefore expect that a tenant
 * they started is withdrawn again. In the window before the rollback, a component that is already live may handle a
 * command (its events stay) or open a subscription query (the rollback ends it). At-least-once handling accepts this.
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
     * <p>
     * If this method throws, the failure rule in the interface documentation applies.
     *
     * @param tenantDescriptor The {@link TenantDescriptor} to register with this component.
     * @return A {@link Registration} used to deregister the given {@code tenantDescriptor}.
     */
    Registration registerTenant(TenantDescriptor tenantDescriptor);

    /**
     * Registers the given {@code tenantDescriptor} as a known tenant with this multi-tenant aware component. If
     * applicable, this task will construct a tenant segment and start it.
     * <p>
     * If this method throws, the failure rule in the interface documentation applies.
     *
     * @param tenantDescriptor The {@link TenantDescriptor} to register with this component.
     * @return A {@link Registration} used to deregister the given {@code tenantDescriptor}.
     */
    Registration registerAndStartTenant(TenantDescriptor tenantDescriptor);
}
