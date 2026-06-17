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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A descriptor for tenants.
 *
 * @param tenantId   The identifier of this tenant.
 * @param properties The properties of this tenant - usually context properties of an Axon Server context.
 *
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public record TenantDescriptor(
        String tenantId,
        Map<String, String> properties
) {

    /**
     * Constructs a TenantDescriptor with the given {@code tenantId}.
     *
     * @param tenantId The identifier of this TenantDescriptor.
     */
    public TenantDescriptor(String tenantId) {
        this(tenantId, Collections.emptyMap());
    }

    /**
     * Constructs a TenantDescriptor with the given {@code tenantId}.
     *
     * @param tenantId The identifier of this TenantDescriptor.
     * @return A TenantDescriptor with the given {@code tenantId}.
     */
    public static TenantDescriptor tenantWithId(String tenantId) {
        return new TenantDescriptor(tenantId);
    }
}
