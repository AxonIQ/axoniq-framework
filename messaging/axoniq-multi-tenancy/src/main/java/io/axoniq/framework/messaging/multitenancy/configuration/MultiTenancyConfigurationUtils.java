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
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.core.Message;

import java.util.List;
import java.util.function.Consumer;

/**
 * Utility class for multi-tenancy configuration.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
public final class MultiTenancyConfigurationUtils {

    /**
     * Fully qualified class names of the multi-tenancy {@link ConfigurationEnhancer ConfigurationEnhancers} living
     * outside this package, referenced as {@code String} rather than as class literals to keep this package free of
     * dependencies on the packages that already depend on it. The
     * {@link ComponentRegistry#disableEnhancer(String)} overload accepts exactly this form.
     * <p>
     * The end-to-end disabling behaviour is covered by the tests of the individual enhancers, so a rename that is not
     * reflected here does not pass unnoticed.
     */
    private static final List<String> EXTERNAL_ENHANCER_CLASS_NAMES = List.of(
            "io.axoniq.framework.messaging.multitenancy.axonserver.configuration.AxonServerMultiTenancyConfigurationDefaults",
            "io.axoniq.framework.messaging.multitenancy.annotation.TenantComponentParameterResolverFactoryConfigurationEnhancer"
    );

    /**
     * Switches multi-tenancy off for the given {@code componentRegistry}.
     * <p>
     * Multi-tenancy is active as soon as the {@code axoniq-multi-tenancy} module is on the classpath, since its
     * {@link ConfigurationEnhancer ConfigurationEnhancers} are contributed through the {@link java.util.ServiceLoader}.
     * Use this method to opt out, for instance in an application that carries the module transitively but runs
     * single-tenant:
     * <pre>{@code
     * MessagingConfigurer.create()
     *                    .componentRegistry(MultiTenancyConfigurationUtils::disableMultiTenancy);
     * }</pre>
     * <p>
     * Disabling only takes effect as long as the multi-tenancy enhancers have not run yet. Applying this method
     * directly to a {@code ComponentRegistry} is therefore always safe, as enhancers run at build time. Applying it
     * from within another {@link ConfigurationEnhancer} requires that enhancer to have a lower
     * {@link ConfigurationEnhancer#order()} than {@link MultiTenancyConfigurationDefaults#ENHANCER_ORDER}.
     *
     * @param componentRegistry the {@link ComponentRegistry} to disable multi-tenancy in
     */
    public static void disableMultiTenancy(ComponentRegistry componentRegistry) {
        componentRegistry.disableEnhancer(MultiTenancyConfigurationDefaults.class);
        EXTERNAL_ENHANCER_CLASS_NAMES.forEach(componentRegistry::disableEnhancer);
    }

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
