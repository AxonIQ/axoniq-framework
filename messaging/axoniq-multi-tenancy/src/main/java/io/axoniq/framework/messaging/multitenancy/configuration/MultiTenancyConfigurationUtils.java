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

import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.messaging.core.Message;

import java.util.function.Consumer;

/**
 * Utility class for multi-tenancy configuration.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
public final class MultiTenancyConfigurationUtils {

    /**
     * Disables the {@link MultiTenancyConfigurationDefaults} enhancer in a {@link ComponentRegistry}.
     */
    public static Consumer<ComponentRegistry> disableMultiTenancyEnhancer = componentRegistry ->
            componentRegistry.disableEnhancer(MultiTenancyConfigurationDefaults.class);


    /**
     * Registers a {@link TenantResolver} for {@link Message}.
     *
     * @param tenantResolver the {@link TenantResolver} to register
     * @return a {@link Consumer} that registers the given {@code tenantResolver} to a {@link ComponentRegistry}
     */
    public static Consumer<ComponentRegistry> registerTenantResolver(TenantResolver<Message> tenantResolver) {
        return componentRegistry -> componentRegistry.registerComponent(
                TenantResolver.class,
                c -> tenantResolver
        );
    }

    private MultiTenancyConfigurationUtils() {
        // utility class
    }
}
