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

import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A descriptor for tenants.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
public class TenantDescriptor {

    private final String tenantId;
    private final Map<String, String> properties;

    /**
     * @param tenantId   The identifier of this tenant.
     * @param properties The (empty) properties of this tenant - usually context properties of an Axon Server context.
     */
    public TenantDescriptor(String tenantId, Map<String, String> properties) {
        this.tenantId = tenantId;
        this.properties = properties;
    }

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

    /**
     * Returns the identifier of this tenant.
     *
     * @return The identifier of this tenant.
     */
    public String tenantId() {
        return tenantId;
    }

    /**
     * Returns the properties of this tenant.
     *
     * @return The properties of this tenant.
     */
    public Map<String, String> properties() {
        return Map.copyOf(properties);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (o == null) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        TenantDescriptor that = (TenantDescriptor) o;

        return Objects.equals(tenantId, that.tenantId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenantId);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TenantDescriptor.class.getSimpleName() + "[", "]")
                .add("tenantId='" + tenantId + "'")
                .add("properties=" + properties)
                .toString();
    }
}
