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

package io.axoniq.framework.messaging.multitenancy;

import io.axoniq.framework.messaging.multitenancy.annotation.TenantComponentParameterResolverFactoryConfigurationEnhancer;
import io.axoniq.framework.messaging.multitenancy.axonserver.configuration.AxonServerMultiTenancyConfigurationDefaults;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationDefaults;
import io.axoniq.framework.messaging.multitenancy.deadletter.DeadLetterMultiTenancyConfigurationEnhancer;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;

import java.util.List;
import java.util.Objects;

/**
 * Utility class containing reusable functionality for configuring multi-tenancy.
 * <p>
 * Multi-tenancy is active as soon as the {@code axoniq-multi-tenancy} module is on the classpath, because its
 * {@link ConfigurationEnhancer ConfigurationEnhancers} are contributed through the
 * {@link java.util.ServiceLoader}. {@link #disable(ComponentRegistry)} is the way to opt out again.
 * <p>
 * This class lives in the module's base package so that every multi-tenancy {@code ConfigurationEnhancer} is a
 * downward dependency and can be referenced as a class literal.
 *
 * @author Jan Galinski
 * @author Jakob Hatzl
 * @since 5.3.0
 */
public final class MultiTenancyUtils {

    /**
     * The multi-tenancy {@link ConfigurationEnhancer ConfigurationEnhancers} that
     * {@link #disable(ComponentRegistry)} switches off.
     * <p>
     * {@link TenantComponentParameterResolverFactoryConfigurationEnhancer} is deliberately absent. It registers the
     * {@link org.axonframework.messaging.core.annotation.ParameterResolverFactory} resolving
     * {@code @TenantScoped} handler parameters, and handler inspection fails the whole configuration when a parameter
     * cannot be resolved. Removing it would stop an application that declares such handlers from starting at all,
     * rather than leaving it to fail per message with a
     * {@link io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException} once no tenant can be
     * resolved. The factory is harmless without tenants: it resolves nothing, because no provider is registered.
     */
    private static final List<Class<? extends ConfigurationEnhancer>> ENHANCERS = List.of(
            MultiTenancyConfigurationDefaults.class,
            AxonServerMultiTenancyConfigurationDefaults.class,
            DeadLetterMultiTenancyConfigurationEnhancer.class
    );

    private MultiTenancyUtils() {
        // Utility class
    }

    /**
     * Switches multi-tenancy off for the given {@code componentRegistry}, leaving it to behave as a single-tenant
     * application.
     * <p>
     * Use this to opt out in an application that carries the {@code axoniq-multi-tenancy} module transitively but runs
     * single-tenant:
     * <pre>{@code
     * MessagingConfigurer.create()
     *                    .componentRegistry(MultiTenancyUtils::disable);
     * }</pre>
     * <p>
     * Only takes effect while the multi-tenancy enhancers have not run yet. Applying this directly to a
     * {@code ComponentRegistry} is therefore always safe, as enhancers run at build time. Applying it from within
     * another {@link ConfigurationEnhancer} requires that enhancer to have a lower
     * {@link ConfigurationEnhancer#order()} than {@link MultiTenancyConfigurationDefaults#ENHANCER_ORDER}.
     *
     * @param componentRegistry the {@link ComponentRegistry} to disable multi-tenancy in
     */
    public static void disable(ComponentRegistry componentRegistry) {
        Objects.requireNonNull(componentRegistry, "The component registry must not be null");
        ENHANCERS.forEach(componentRegistry::disableEnhancer);
    }

    /**
     * Returns every multi-tenancy {@link ConfigurationEnhancer} that {@link #disable(ComponentRegistry)} switches off.
     *
     * @return every multi-tenancy {@link ConfigurationEnhancer} that {@link #disable(ComponentRegistry)} switches off
     */
    static List<Class<? extends ConfigurationEnhancer>> enhancers() {
        return ENHANCERS;
    }
}
