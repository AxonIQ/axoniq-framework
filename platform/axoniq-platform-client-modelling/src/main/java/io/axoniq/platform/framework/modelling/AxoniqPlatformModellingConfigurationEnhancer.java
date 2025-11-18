/*
 * Copyright (c) 2022-2025. AxonIQ B.V.
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

import io.axoniq.platform.framework.AxoniqPlatformProperties;
import io.axoniq.platform.framework.UtilsKt;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.modelling.StateManager;
import org.axonframework.modelling.repository.Repository;

import static io.axoniq.platform.framework.AxoniqPlatformConfigurerEnhancer.PLATFORM_ENHANCER_ORDER;

public class AxoniqPlatformModellingConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        if (!registry.hasComponent(AxoniqPlatformProperties.class)) {
            return;
        }
        registry

                .registerDecorator(DecoratorDefinition.forType(StateManager.class)
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


    @Override
    public int order() {
        return PLATFORM_ENHANCER_ORDER;
    }
}
