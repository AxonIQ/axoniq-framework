/*
 * Copyright (c) 2022-2026. AxonIQ B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
