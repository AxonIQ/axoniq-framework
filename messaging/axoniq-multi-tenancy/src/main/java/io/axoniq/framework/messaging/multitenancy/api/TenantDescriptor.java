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

import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

/**
 * A descriptor for tenants holding the {@code tenantId} and a {@link Map} of provider specific properties that
 * can be used for tenant resolution.
 * <p>
 * The identity of this class does on purpose only include the {@code #tenantId} to allow using it as map key.
 *
 * @author Stefan Dragisic
 * @author Jan Galinski
 * @since 4.6.0
 */
public class TenantDescriptor {

    /**
     * The key under which a tenant identifier is stored in a message's {@link org.axonframework.messaging.core.Metadata
     * metadata}.
     */
    public static final String TENANT_ID_KEY = "tenantId";

    /**
     * The {@link ResourceKey} used whenever a {@link ProcessingContext} would contain a {@code TenantDescriptor}.
     */
    public static final ResourceKey<TenantDescriptor> RESOURCE_KEY = ResourceKey.withLabel(TENANT_ID_KEY);

    private final String tenantId;
    private final Map<String, String> properties;

    /**
     * Creates a new {@code TenantDescriptor} with the given {@code tenantId} and {@code properties}.
     *
     * @param tenantId   the identifier of this {@code TenantDescriptor}
     * @param properties the (empty) properties of this tenant, usually provider specific properties that can be used
     *                   for tenant resolution
     */
    public TenantDescriptor(String tenantId, Map<String, String> properties) {
        this.tenantId = requireNonNull(tenantId, "tenantId must not be null");
        this.properties = requireNonNull(Map.copyOf(properties), "properties must not be null");
    }

    /**
     * Constructs a {@code TenantDescriptor} with the given {@code tenantId}.
     *
     * @param tenantId the identifier of this {@code TenantDescriptor}
     */
    public TenantDescriptor(String tenantId) {
        this(tenantId, Collections.emptyMap());
    }

    /**
     * Constructs a {@code TenantDescriptor} with the given {@code tenantId}.
     *
     * @param tenantId the identifier of this {@code TenantDescriptor}
     * @return a {@code TenantDescriptor} with the given {@code tenantId}
     */
    public static TenantDescriptor tenantWithId(String tenantId) {
        return new TenantDescriptor(tenantId);
    }

    /**
     * Returns an {@link Optional} of the {@code TenantDescriptor} kept under the {@link #RESOURCE_KEY} in the given
     * {@code context}, or {@link Optional#empty()} when none is present.
     *
     * @param context the {@link ProcessingContext} to retrieve the {@code TenantDescriptor} from, if present
     * @return an {@link Optional} of the {@code TenantDescriptor} kept under the {@link #RESOURCE_KEY} in the given
     * {@code context}
     */
    public static Optional<TenantDescriptor> fromContext(ProcessingContext context) {
        return Optional.ofNullable(context.getResource(RESOURCE_KEY));
    }

    /**
     * Returns the identifier of this {@code TenantDescriptor}.
     *
     * @return the identifier of this {@code TenantDescriptor}
     */
    public String tenantId() {
        return tenantId;
    }

    /**
     * Returns the properties of this {@code TenantDescriptor}.
     *
     * @return the properties of this {@code TenantDescriptor}
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
