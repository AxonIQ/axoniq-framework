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

package org.axonframework.messaging.core.reflection;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.core.annotation.HierarchicalParameterResolverFactory;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;

import java.util.Optional;

/**
 * {@link ConfigurationEnhancer} that registers a decorator for the {@link ParameterResolverFactory} that, when a parent
 * configuration is present, wraps both the parent and the current {@link ParameterResolverFactory} in a
 * {@link HierarchicalParameterResolverFactory} that delegates to the parent if a parameter cannot be resolved by the
 * current configuration.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 * @see HierarchicalParameterResolverFactory
 */
public class HierarchicalParameterResolverFactoryConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        componentRegistry.registerDecorator(
                ParameterResolverFactory.class,
                // We want this to be executed late, but still allow users to be able to add resolvers
                // after this enhancer. Which would then not be available for child configurations.
                Integer.MAX_VALUE >> 1,
                (config, componentName, component) -> {
                    Optional<ParameterResolverFactory> parentComponent = Optional
                            .ofNullable(config.getParent())
                            .flatMap(p -> p.getOptionalComponent(ParameterResolverFactory.class));
                    if (parentComponent.isPresent()) {
                        return HierarchicalParameterResolverFactory.create(
                                parentComponent.get(),
                                component
                        );
                    }
                    return component;
                });
    }
}
