/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
