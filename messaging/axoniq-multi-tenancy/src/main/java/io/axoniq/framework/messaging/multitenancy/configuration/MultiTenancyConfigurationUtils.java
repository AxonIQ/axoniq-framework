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

import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.messaging.core.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Utility class for multi-tenancy configuration.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
public final class MultiTenancyConfigurationUtils {

    private static final Logger logger = LoggerFactory.getLogger(MultiTenancyConfigurationUtils.class);

    /**
     * To selectively enable the {@link MultiTenancyConfigurationDefaults} enhancer in a {@link ComponentRegistry}, use
     * this record to indicate whether multi-tenancy is enabled or disabled.
     * TODO: We need a more general approach to control this, see issue #258.
     */
    public enum MultiTenancyEnabled {
        /**
         * Indicates that multi-tenancy is enabled in the {@link ComponentRegistry}.
         */
        ENABLED;

        /**
         * Enables the {@link MultiTenancyConfigurationDefaults} enhancer in a {@link ComponentRegistry}.
         *
         * @param componentRegistry the {@link ComponentRegistry} to enable the enhancer in
         */
        public static void enableMultiTenancyEnhancer(ComponentRegistry componentRegistry) {
            componentRegistry.registerIfNotPresent(MultiTenancyEnabled.class, c -> MultiTenancyEnabled.ENABLED);
        }

        /**
         * Checks whether multi-tenancy is enabled in a {@link ComponentRegistry}.
         *
         * @param componentRegistry the {@link ComponentRegistry} to check
         * @return {@code true} if multi-tenancy is enabled, {@code false} otherwise
         */
        public static boolean isEnabled(ComponentRegistry componentRegistry) {
            if (componentRegistry.hasComponent(MultiTenancyEnabled.class)) {
                return true;
            }
            logger.info(
                    "Multi-tenancy is disabled. To enable it, register the MultiTenancyEnabled component in the ComponentRegistry.");
            return false;
        }
    }

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
    public static Consumer<ComponentRegistry> registerTenantResolver(TenantResolver tenantResolver) {
        return componentRegistry -> componentRegistry.registerComponent(
                TenantResolver.class,
                c -> tenantResolver
        );
    }

    /**
     * Registers a {@link TenantConnectPredicate} for {@link Message}.
     *
     * @param predicate the {@link TenantConnectPredicate} to register
     * @return a {@link Consumer} that registers the given {@code predicate} to a {@link ComponentRegistry}
     */
    public static Consumer<ComponentRegistry> registerTenantConnectPredicate(TenantConnectPredicate predicate) {
        return componentRegistry -> componentRegistry.registerComponent(
                TenantConnectPredicate.class,
                c -> predicate
        );
    }

    private MultiTenancyConfigurationUtils() {
        // utility class
    }
}
