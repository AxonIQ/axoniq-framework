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

package org.axonframework.modelling;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;

import java.util.Optional;

/**
 * {@link ConfigurationEnhancer} that registers a decorator for the {@link StateManager} that, when a parent
 * configuration is present, wraps child and parent {@link StateManager} in a {@link HierarchicalStateManager} that
 * delegates to the parent if a state cannot be resolved by the current configuration.
 * <p>
 * To prevent modules not defining entities not being able to find the {@link StateManager} component to access the
 * parent configuration, an empty {@link StateManager} is registered when no {@link StateManager} is present.
 * <p>
 * This enhancer is registered with the highest order, so it will be executed last. This is to ensure that any
 * registered {@link StateManager} is decorated with the {@link HierarchicalStateManager}.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class HierarchicalStateManagerConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        componentRegistry.registerDecorator(
                StateManager.class,
                Integer.MAX_VALUE,
                (config, componentName, component) -> {
                    Optional<StateManager> parentComponent = Optional
                            .ofNullable(config.getParent())
                            .flatMap(p -> p.getOptionalComponent(StateManager.class));
                    if (parentComponent.isPresent()) {
                        return HierarchicalStateManager.create(parentComponent.get(), component);
                    }
                    return component;
                });
    }
}
