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

package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import org.axonframework.common.configuration.ComponentRegistry;

import java.util.Set;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;

/**
 * Utility class for multi-tenancy integration tests. Common configuration helpers for multi-tenancy test setups are
 * provided here.
 *
 * @author Jan Galinski
 */
final class TenantFixture {

    /**
     * A {@link TenantConnectPredicate} that only allows custom tenants to be connected. The default and admin tenants
     * are excluded.
     */
    static final TenantConnectPredicate CONNECT_ONLY_CUSTOM_TENANTS =
            tenantDescriptor -> !Set.of(ADMIN_CONTEXT, DEFAULT_CONTEXT).contains(tenantDescriptor.tenantId());


    private TenantFixture() {
        // utility class
    }

    /**
     * Registers a {@link TenantConnectPredicate} that only connects to tenants whose ID starts with the given
     * {@code prefix}, on top of excluding the default and admin contexts. Scoped to a prefix rather than every
     * custom context, because the shared Axon Server test container this runs against is also used concurrently by
     * other test classes in this module, each creating their own, differently-prefixed custom contexts.
     */
    public static void connectOnlyCustomTenantsPredicate(ComponentRegistry registry, String prefix) {
        registry.registerComponent(
                TenantConnectPredicate.class,
                config -> descriptor -> CONNECT_ONLY_CUSTOM_TENANTS.test(descriptor)
                                        && descriptor.tenantId().startsWith(prefix));
    }
}
