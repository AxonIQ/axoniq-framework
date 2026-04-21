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

package io.axoniq.platform.framework.modelling;

import io.axoniq.platform.framework.UtilsKt;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.modelling.StateManager;
import org.axonframework.modelling.repository.Repository;

/**
 * Holder of the actual decorator registrations against {@code axon-modelling} types. Kept separate from
 * {@link AxoniqPlatformModellingConfigurationEnhancer} so that the enhancer class can be loaded even when
 * {@code axon-modelling} is not on the classpath — this class is only touched after a {@code Class.forName}
 * probe confirms the module is present.
 */
final class ModellingDecorators {

    private ModellingDecorators() {
    }

    static void apply(ComponentRegistry registry) {
        registry.registerDecorator(DecoratorDefinition.forType(StateManager.class)
                                                      .with((cc, name, delegate) ->
                                                                    new AxoniqPlatformStateManager(delegate))
                                                      .order(Integer.MAX_VALUE));

        UtilsKt.doOnSubModules(registry, (componentRegistry, module) -> {
            componentRegistry
                    .registerDecorator(DecoratorDefinition.forType(Repository.class)
                                                          .with((cc, name, delegate) ->
                                                                        new AxoniqPlatformRepository<>(delegate))
                                                          .order(Integer.MIN_VALUE))
                    .registerDecorator(DecoratorDefinition.forType(StateManager.class)
                                                          .with((cc, name, delegate) ->
                                                                        new AxoniqPlatformStateManager(delegate))
                                                          .order(Integer.MAX_VALUE));
            return null;
        });
    }
}
