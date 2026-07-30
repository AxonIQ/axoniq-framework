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

package io.axoniq.framework.messaging.multitenancy.axonserver.api;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;

/**
 * A {@link TenantConnectPredicate} that always receives contexts from the Axon Server and returns {@code true}, unless
 * it is the {@link AxonServerConfiguration#ADMIN_CONTEXT}.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
public class AxonServerTenantConnectPredicate implements TenantConnectPredicate {

    @Override
    public boolean test(TenantDescriptor tenantDescriptor) {
        return !ADMIN_CONTEXT.equals(tenantDescriptor.tenantId());
    }
}
