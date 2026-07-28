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

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.correlation.CorrelationDataProvider;
import org.axonframework.messaging.core.correlation.SimpleCorrelationDataProvider;

import java.util.Collection;
import java.util.Optional;

import static org.axonframework.common.BuilderUtils.assertNonEmpty;

/**
 * A {@link TenantResolver} implementation that resolves the target tenant from message metadata.
 * <p>
 * This resolver extracts the tenant identifier from the message's {@link Message#metadata() metadata} using a
 * configurable key (default: {@code "tenantId"}). If the metadata does not contain the expected key, a
 * {@link TenantNotResolvedException} is thrown.
 * <p>
 * This is the standard resolver for metadata-based multi-tenant routing. Combined with a
 * {@link CorrelationDataProvider} that propagates the same metadata key, this enables automatic tenant context
 * propagation throughout the message handling chain.
 * <p>
 * Example usage:
 * <pre><code>
 *     // Using default metadata key "tenantId"
 *     TenantResolver resolver = new MetadataBasedTenantResolver();
 *
 *     // Using custom metadata key
 *     TenantResolver resolver = new MetadataBasedTenantResolver("customTenantKey");
 * </code></pre>
 *
 * @param metadataKey the key to use when extracting the tenant identifier from message metadata, default is
 *                    {@code "tenantId"}
 * @author Theo Emanuelsson
 * @author Jan Galinski
 * @see TenantResolver
 * @see SimpleCorrelationDataProvider
 * @since 5.3.0
 */
public record MetadataBasedTenantResolver(String metadataKey) implements TenantResolver {

    /**
     * The default metadata key used to store the tenant identifier.
     */
    public static final String DEFAULT_TENANT_METADATA_KEY = TenantUtils.TENANT_ID_KEY;

    /**
     * Constructs a {@code MetadataBasedTenantResolver} using the default metadata key {@code "tenantId"}.
     */
    public MetadataBasedTenantResolver() {
        this(DEFAULT_TENANT_METADATA_KEY);
    }

    /**
     * Constructs a {@code MetadataBasedTenantResolver} using the specified metadata key.
     *
     * @param metadataKey the key to use when extracting the tenant identifier from message metadata, must not be
     *                    {@code null} or empty
     */
    public MetadataBasedTenantResolver {
        assertNonEmpty(metadataKey, "The metadata key must not be null or empty");
    }

    /**
     * Resolves the target tenant by extracting the tenant identifier from the message's metadata.
     *
     * @param message the message to resolve the tenant from
     * @param tenants the available tenants
     * @return the {@link TenantDescriptor} for the resolved tenant
     * @throws TenantNotResolvedException if the message metadata does not contain the expected tenant key
     */
    @Override
    public TenantDescriptor resolveTenant(Message message,
                                          Collection<TenantDescriptor> tenants
    ) {
        String tenantId = message.metadata().get(metadataKey);
        if (tenantId == null) {
            throw new TenantNotResolvedException(
                    "No tenant identifier found in message metadata under key '%s'", metadataKey
            );
        }
        Optional<TenantDescriptor> resolvedTenant = tenants.stream()
                                                           .filter(tenant -> tenantId.equals(tenant.tenantId()))
                                                           .findFirst();
        return resolvedTenant.orElseGet(() -> TenantDescriptor.tenantWithId(tenantId));
    }
}
