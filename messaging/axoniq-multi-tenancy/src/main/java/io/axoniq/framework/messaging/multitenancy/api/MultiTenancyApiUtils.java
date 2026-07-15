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
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException.tenantNotResolved;

/**
 * Utility class for multi-tenancy API.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
public final class MultiTenancyApiUtils {

    /**
     * The key used to store the {@link TenantDescriptor} in a {@link ProcessingContext} or {@link Metadata}.
     */
    public static final String TENANT_ID_KEY = "tenantId";

    /**
     * The {@link ResourceKey} used to store the {@link TenantDescriptor} in a {@link ProcessingContext}.
     */
    public static final ResourceKey<TenantDescriptor> TENANT_RESOURCE_KEY = ResourceKey.withLabel(TENANT_ID_KEY);

    /**
     * A {@link Function} that retrieves the {@link TenantDescriptor} from a {@link ProcessingContext}. If no
     *
     * @param processingContext the {@link ProcessingContext} to retrieve the {@link TenantDescriptor} from
     * @return the       {@link TenantDescriptor} from the {@link ProcessingContext}
     * @throws TenantNotResolvedException if no {@link TenantDescriptor} is found in the {@link ProcessingContext}
     */
    public static TenantDescriptor getTenantDescriptor(ProcessingContext processingContext)
            throws TenantNotResolvedException {
        return Optional
                .ofNullable(processingContext.getResource(TENANT_RESOURCE_KEY))
                .orElseThrow(tenantNotResolved("No tenant descriptor found in processing context"));
    }

    /**
     * A {@link BiConsumer} that sets the {@link TenantDescriptor} in a {@link ProcessingContext}.
     *
     * @param processingContext the {@link ProcessingContext} to set the {@link TenantDescriptor} in
     * @param tenantDescriptor  the {@link TenantDescriptor} to set in the {@link ProcessingContext}
     * @return the previous {@link TenantDescriptor} in the {@link ProcessingContext}
     */
    @Nullable
    public static TenantDescriptor setTenantDescriptor(ProcessingContext processingContext,
                                                       TenantDescriptor tenantDescriptor) {
        return processingContext.putResource(TENANT_RESOURCE_KEY, tenantDescriptor);
    }

    private MultiTenancyApiUtils() {
        // utility class
    }
}
