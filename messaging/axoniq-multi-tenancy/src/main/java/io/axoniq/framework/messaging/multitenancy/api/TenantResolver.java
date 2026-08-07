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

import org.axonframework.messaging.core.Message;

import java.util.Collection;
import java.util.Collections;

/**
 * Resolves the target tenant of a given {@link Message}, and attaches a tenant to a {@link Message} in the
 * inverse direction.
 * <p>
 * A message dispatched from within a handler generally does not name its own tenant: the tenant is only
 * known through the {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} of the message
 * being handled. {@link #attachTenant(Message, TenantDescriptor)} is what makes that tenant survive the
 * message being dispatched elsewhere, for example across a distributed command or query bus that carries
 * the message to another process and back. A resolver that only resolves, and never attaches, silently
 * drops the tenant on every such dispatch, with no signal that anything is missing until a receiving
 * component fails to resolve a tenant it should have had.
 *
 * @author Stefan Dragisic
 * @author Jan Galinski
 * @author Jakob Hatzl
 * @since 4.6.0
 */
public interface TenantResolver {

    /**
     * Returns {@link TenantDescriptor} for the given {@code message}.
     *
     * @param message the {@link Message} implementation to resolve the target tenant for
     * @param tenants the collection of tenants to resolve the target tenant from
     * @return the resolved {@link TenantDescriptor} based on the given {@code message}
     * @throws TenantNotResolvedException if no tenant could be resolved
     */
    TenantDescriptor resolveTenant(
            Message message,
            Collection<TenantDescriptor> tenants
    ) throws TenantNotResolvedException;

    /**
     * Returns {@link TenantDescriptor} for the given {@code message}. This method is a convenience method that calls
     * {@link #resolveTenant(Message, Collection)} with an empty collection of tenants.
     *
     * @param message the {@link Message} implementation to resolve the target tenant for
     * @return the resolved {@link TenantDescriptor} based on the given {@code message}
     * @throws TenantNotResolvedException if no tenant could be resolved
     * @see #resolveTenant(Message, Collection)
     */
    default TenantDescriptor resolveTenant(Message message) {
        return resolveTenant(message, Collections.emptyList());
    }

    /**
     * Returns a copy of the given {@code message} carrying the given {@code tenant}, the inverse of
     * {@link #resolveTenant(Message, Collection)}: where that method determines the tenant a message
     * belongs to, this one makes that determination survive the message being dispatched elsewhere.
     *
     * @param message the message to attach the given {@code tenant} to
     * @param tenant  the tenant to attach to the given {@code message}
     * @return a copy of the given {@code message} carrying the given {@code tenant}
     */
    Message attachTenant(Message message, TenantDescriptor tenant);
}
