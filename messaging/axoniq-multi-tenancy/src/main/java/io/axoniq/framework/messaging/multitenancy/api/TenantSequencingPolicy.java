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
import org.axonframework.messaging.core.sequencing.SequencingPolicy;

import java.util.Objects;

/**
 * A {@link SequencingPolicy} that sequences messages by the tenant resolved for message processing.
 * <p>
 * Messages resolving to the same tenant use the same sequence identifier. Messages resolving to different tenants can
 * be handled independently. When no known tenant can be resolved, this policy returns no sequence identifier.
 *
 * @author Jan Galinski
 * @since 5.4.0
 */
@FunctionalInterface
public interface TenantSequencingPolicy extends SequencingPolicy<Message> {

    /**
     * Creates a {@code TenantSequencingPolicy} backed by the given {@code tenantRouter}.
     *
     * @param tenantRouter the tenant router used to resolve known tenants
     * @return a tenant sequencing policy backed by the given {@code tenantRouter}
     */
    static TenantSequencingPolicy from(TenantRouter tenantRouter) {
        Objects.requireNonNull(tenantRouter, "The tenant router must not be null");
        return (message, context) -> tenantRouter.resolveFromContext(context)
                                                .or(() -> tenantRouter.resolveFromMessage(message))
                                                .map(TenantDescriptor::tenantId);
    }
}
