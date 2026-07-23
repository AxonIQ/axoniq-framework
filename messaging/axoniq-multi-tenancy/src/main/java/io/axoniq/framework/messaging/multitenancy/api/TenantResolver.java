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
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * Resolves the target tenant of a given {@link Message}.
 *
 * @author Stefan Dragisic
 * @author Jan Galinski
 * @since 4.6.0
 */
@FunctionalInterface
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
     * Resolves the tenant from the current {@link ProcessingContext} by extracting the message stored on it and passing
     * it to the given {@code resolver}.
     * <p>
     * This is a convenience method for components that operate within an existing processing context (such as the event
     * store or snapshot store) and need to resolve the tenant from the context's message rather than from a directly
     * available message parameter.
     *
     * @param context the processing context containing the message
     * @param tenants the collection of known tenants
     * @return the resolved {@link TenantDescriptor}
     * @throws IllegalStateException if no message is found in the processing context
     */
    default TenantDescriptor resolveTenant(ProcessingContext context, Collection<TenantDescriptor> tenants) {
        Message message = Optional.ofNullable(Message.fromContext(context))
                                  .orElseThrow(() -> new IllegalStateException(
                                          "Cannot resolve tenant: no message found in ProcessingContext"));

        return resolveTenant(message, tenants);
    }
}
