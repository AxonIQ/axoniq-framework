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

package org.axonframework.modelling.configuration;

import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.modelling.SimpleStateManager;
import org.axonframework.modelling.StateManager;

import java.util.Objects;

/**
 * A {@link ConfigurationEnhancer} registering the default components of the {@link ModellingConfigurer}.
 * <p>
 * Will only register the following components <b>if</b> there is no component registered for the given class yet:
 * <ul>
 *     <li>Registers a {@link SimpleStateManager} for class {@link StateManager}</li>
 * </ul>
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class ModellingConfigurationDefaults implements ConfigurationEnhancer {

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void enhance(ComponentRegistry registry) {
        Objects.requireNonNull(registry, "Cannot enhance a null ComponentRegistry.");

        registry.registerIfNotPresent(ComponentDefinition
                                              .ofType(StateManager.class)
                                              .withBuilder(c -> SimpleStateManager.named("DefaultStateManager")));
    }
}
