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

package io.axoniq.framework.examples.cli;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;

/**
 * List of tenants used in this example for easier use in REPL.
 * Note: Tenants must exists as contexts on the connected axon server.
 */
public enum Tenants {
    /**
     * Tenant A
     */
    A("foo-a"),
    /**
     * Tenant B
     */
    B("foo-b"),
    ;

    private final TenantDescriptor tenantDescriptor;

    Tenants(String tenantId) {
        this.tenantDescriptor = TenantDescriptor.tenantWithId(tenantId);
    }

    public TenantDescriptor tenantDescriptor() {
        return tenantDescriptor;
    }

    public String tenantId() {
        return tenantDescriptor().tenantId();
    }

}
