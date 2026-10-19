/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.axonserver.api;

import io.axoniq.axonserver.grpc.admin.ContextOverview;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.common.annotation.Internal;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for Axon Server multi-tenancy features.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
@Internal
final class AxonServerTenantUtils {

    /**
     * Extract {@link TenantDescriptor} information from the given {@link ContextOverview}.
     *
     * @param contextOverview the {@link ContextOverview} to extract the {@link TenantDescriptor} from
     * @return the {@link TenantDescriptor} extracted from the given {@link ContextOverview}
     */
    static TenantDescriptor tenantDescriptor(ContextOverview contextOverview) {
        Map<String, String> properties = new HashMap<>(contextOverview.getMetaDataMap());
        properties.putIfAbsent("replicationGroup", contextOverview.getReplicationGroup().getName());

        return new TenantDescriptor(
                contextOverview.getName(),
                properties
        );
    }

    private AxonServerTenantUtils() {
        // utility class
    }
}
