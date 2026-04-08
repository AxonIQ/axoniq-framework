/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.core.configuration.reflection;

import org.axonframework.common.configuration.ComponentDecorator;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.messaging.core.annotation.MultiParameterResolverFactory;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;

import java.util.Objects;
import java.util.function.Function;

/**
 * Utility class that provides methods to register a {@link ParameterResolverFactory} to the {@link ComponentRegistry}.
 * <p>
 * Ensures that the {@code ComponentRegistry} at all times has <b>one</b> {@code ParameterResolverFactory} component.
 * Subsequent invocations of
 * {@link #registerToComponentRegistry(ComponentRegistry, Function)}/{@link
 * #registerToComponentRegistry(ComponentRegistry, int, Function)} will
 * {@link ComponentRegistry#registerDecorator(Class, String, int, ComponentDecorator) decorate} the existing
 * {@code ParameterResolverFactory} and given {@code ParameterResolverFactory} into a
 * {@link MultiParameterResolverFactory}.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class ParameterResolverFactoryUtils {

    /**
     * Register a {@link ParameterResolverFactory} to the {@link SearchScope#CURRENT current} {@link ComponentRegistry}
     * using the given {@code factory} function. It will be registered with order {@code 0}.
     *
     * @param componentRegistry The {@link ComponentRegistry} to register the {@link ParameterResolverFactory} to.
     * @param factory           The {@link Function} that creates the {@link ParameterResolverFactory} based on the
     *                          {@link Configuration}.
     */
    public static void registerToComponentRegistry(ComponentRegistry componentRegistry,
                                                   Function<Configuration, ParameterResolverFactory> factory
    ) {
        Objects.requireNonNull(componentRegistry, "ComponentRegistry cannot be null");
        registerToComponentRegistry(componentRegistry, 0, factory);
    }

    /**
     * Register a {@link ParameterResolverFactory} to the {@link SearchScope#CURRENT current} {@link ComponentRegistry}
     * using the given {@code factory} function.
     *
     * @param componentRegistry The {@link ComponentRegistry} to register the {@link ParameterResolverFactory} to.
     * @param order             The order in which the {@link ParameterResolverFactory} should be registered.
     * @param factory           The {@link Function} that creates the {@link ParameterResolverFactory} based on the
     *                          {@link Configuration}.
     */
    public static void registerToComponentRegistry(ComponentRegistry componentRegistry,
                                                   int order,
                                                   Function<Configuration, ParameterResolverFactory> factory
    ) {
        Objects.requireNonNull(componentRegistry, "ComponentRegistry cannot be null");
        Objects.requireNonNull(factory, "Factory cannot be null");

        if (!componentRegistry.hasComponent(ParameterResolverFactory.class, SearchScope.CURRENT)) {
            componentRegistry.registerComponent(ParameterResolverFactory.class, factory::apply);
            return;
        }
        componentRegistry.registerDecorator(
                ParameterResolverFactory.class,
                order,
                (config, componentName, component) -> MultiParameterResolverFactory.ordered(
                        component, factory.apply(config)
                )
        );
    }

    private ParameterResolverFactoryUtils() {
        // Utility class
    }
}
