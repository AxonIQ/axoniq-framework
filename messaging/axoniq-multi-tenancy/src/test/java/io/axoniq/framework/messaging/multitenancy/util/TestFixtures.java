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

package io.axoniq.framework.messaging.multitenancy.util;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptors;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.messaging.core.Message;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_REPLICATION_GROUP;
import static io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor.tenantWithId;

/**
 * Utility class providing test fixtures for multi-tenancy related tests.
 */
public enum TestFixtures {
    ;

    public static final TenantDescriptor TENANT_A = new TenantDescriptor(
            "foo-a",
            Map.of("replicationGroup", DEFAULT_REPLICATION_GROUP)
    );

    public static final TenantDescriptor TENANT_B = new TenantDescriptor(
            "foo-b",
            Map.of("replicationGroup", DEFAULT_REPLICATION_GROUP)
    );

    public static final List<TenantDescriptor> TENANT_LIST = List.of(TENANT_A, TENANT_B);
    public static final TenantDescriptors TENANT_DESCRIPTORS = () -> TENANT_LIST;

    /**
     * Creates a {@link TenantResolver} that always resolves to the given tenant ID.
     *
     * @param tenantId the tenant ID to resolve to
     * @return a {@link TenantResolver} that always resolves to the given tenant ID
     * @see #alwaysTenant(TenantDescriptor)
     */
    public static TenantResolver alwaysTenant(String tenantId) {
        return alwaysTenant(tenantWithId(tenantId));
    }

    /**
     * Creates a {@link TenantResolver} that always resolves to the given {@link TenantDescriptor}.
     *
     * @param tenantDescriptor the {@link TenantDescriptor} to resolve to
     * @return a {@link TenantResolver} that always resolves to the given {@link TenantDescriptor}
     */
    public static TenantResolver alwaysTenant(TenantDescriptor tenantDescriptor) {
        return new AlwaysTenant(tenantDescriptor);
    }

    private record AlwaysTenant(TenantDescriptor tenantDescriptor) implements TenantResolver {

        @Override
        public TenantDescriptor resolveTenant(Message message, Collection<TenantDescriptor> tenants) {
            return tenantDescriptor;
        }

        @Override
        public Message attachTenant(Message message, TenantDescriptor tenant) {
            return message.andMetadata(Map.of(TenantDescriptor.TENANT_ID_KEY, tenant.tenantId()));
        }
    }
}
