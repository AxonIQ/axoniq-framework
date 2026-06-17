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

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.messaging.multitenancy.api.NoSuchTenantException;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.correlation.CorrelationDataProvider;
import org.axonframework.messaging.core.correlation.SimpleCorrelationDataProvider;

import java.util.Collection;
import java.util.Optional;

import static org.axonframework.common.BuilderUtils.assertNonEmpty;

/**
 * A {@link TenantResolver} implementation that resolves the target tenant from message metadata.
 * <p>
 * This resolver extracts the tenant identifier from the message's {@link Message#metadata() metadata}
 * using a configurable key (default: {@code "tenantId"}). If the metadata does not contain the
 * expected key, a {@link NoSuchTenantException} is thrown.
 * <p>
 * This is the standard resolver for metadata-based multi-tenant routing. Combined with a
 * {@link CorrelationDataProvider} that propagates
 * the same metadata key, this enables automatic tenant context propagation throughout the
 * message handling chain.
 * <p>
 * Example usage:
 * <pre><code>
 *     // Using default metadata key "tenantId"
 *     TenantResolver&lt;Message&gt; resolver = new MetadataBasedTenantResolver();
 *
 *     // Using custom metadata key
 *     TenantResolver&lt;Message&gt; resolver = new MetadataBasedTenantResolver("customTenantKey");
 * </code></pre>
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 * @see TenantResolver
 * @see SimpleCorrelationDataProvider
 */
public class MetadataBasedTenantResolver implements TenantResolver<Message> {

    /**
     * The default metadata key used to store the tenant identifier.
     */
    public static final String DEFAULT_TENANT_KEY = "tenantId";

    private final String metadataKey;

    /**
     * Constructs a {@link MetadataBasedTenantResolver} using the default metadata key {@code "tenantId"}.
     */
    public MetadataBasedTenantResolver() {
        this(DEFAULT_TENANT_KEY);
    }

    /**
     * Constructs a {@link MetadataBasedTenantResolver} using the specified metadata key.
     *
     * @param metadataKey the key to use when extracting the tenant identifier from message metadata,
     *                    must not be {@code null} or empty
     */
    public MetadataBasedTenantResolver(String metadataKey) {
        assertNonEmpty(metadataKey, "The metadata key must not be null or empty");
        this.metadataKey = metadataKey;
    }

    /**
     * Resolves the target tenant by extracting the tenant identifier from the message's metadata.
     *
     * @param message the message to resolve the tenant from
     * @param tenants the available tenants (not used by this implementation)
     * @return the {@link TenantDescriptor} for the resolved tenant
     * @throws NoSuchTenantException if the message metadata does not contain the expected tenant key
     */
    @Override
    public TenantDescriptor apply(Message message, Collection<TenantDescriptor> tenants) {
        String tenantId = message.metadata().get(metadataKey);
        if (tenantId == null) {
            throw new NoSuchTenantException(
                    "No tenant identifier found in message metadata under key '" + metadataKey + "'"
            );
        }
        Optional<TenantDescriptor> resolvedTenant = tenants.stream()
                                                           .filter(tenant -> tenantId.equals(tenant.tenantId()))
                                                           .findFirst();
        return resolvedTenant.orElseGet(() -> TenantDescriptor.tenantWithId(tenantId));
    }

    /**
     * Returns the metadata key used by this resolver.
     *
     * @return the metadata key
     */
    public String metadataKey() {
        return metadataKey;
    }
}
