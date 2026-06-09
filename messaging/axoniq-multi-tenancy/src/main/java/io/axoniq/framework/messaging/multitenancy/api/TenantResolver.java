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
import java.util.function.BiFunction;

/**
 * Resolves the target tenant of a given {@link Message} implementation of type {@code M}.
 *
 * @param <M> The {@link Message} implementation this resolver acts on.
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public interface TenantResolver<M extends Message>
        extends BiFunction<M, Collection<TenantDescriptor>, TenantDescriptor> {

    /**
     * Returns {@link TenantDescriptor} for the given {@code message}.
     *
     * @param message The {@link Message} implementation to resolve the target tenant for.
     * @param tenants The collection of tenants to resolve the target tenant from. May be empty.
     * @return The resolved {@link TenantDescriptor} based on the given {@code message}.
     */
    default TenantDescriptor resolveTenant(M message, Collection<TenantDescriptor> tenants) {
        return this.apply(message, Collections.unmodifiableCollection(tenants));
    }
}
