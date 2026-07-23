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

package io.axoniq.framework.messaging.multitenancy.axonserver;

import io.axoniq.axonserver.grpc.admin.ContextOverview;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for Axon Server multi-tenancy features.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
public final class AxonServerTenantUtils {

    /**
     * Extract {@link TenantDescriptor} information from the given {@link ContextOverview}.
     *
     * @param contextOverview the {@link ContextOverview} to extract the {@link TenantDescriptor} from
     * @return the {@link TenantDescriptor} extracted from the given {@link ContextOverview}
     */
    public static TenantDescriptor tenantDescriptor(ContextOverview contextOverview) {
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
