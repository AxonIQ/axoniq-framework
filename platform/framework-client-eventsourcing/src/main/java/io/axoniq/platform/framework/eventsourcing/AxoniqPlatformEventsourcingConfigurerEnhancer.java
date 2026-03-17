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

package io.axoniq.platform.framework.eventsourcing;

import io.axoniq.platform.framework.messaging.HandlerMetricsRegistry;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;

import static io.axoniq.platform.framework.AxoniqPlatformConfigurerEnhancer.PLATFORM_ENHANCER_ORDER;

public class AxoniqPlatformEventsourcingConfigurerEnhancer implements ConfigurationEnhancer {

    /**
     * This enhancer should be configured after the platform enhancer, to have the HandlerMetricRegistry component
     * available.
     */
    public static final int EVENTSOURCING_ENHANCER_ORDER = PLATFORM_ENHANCER_ORDER + 1;

    @Override
    public void enhance(ComponentRegistry registry) {
        if (!registry.hasComponent(HandlerMetricsRegistry.class)) {
            return;
        }
        registry.registerDecorator(
                DecoratorDefinition.forType(EventStorageEngine.class)
                                   .with((c, name, delegate) -> {
                                       HandlerMetricsRegistry metricsRegistry = c.getComponent(HandlerMetricsRegistry.class);
                                       return new AxoniqPlatformEventStorageEngine(delegate, metricsRegistry);
                                   }).order(Integer.MAX_VALUE));
    }


    @Override
    public int order() {
        return EVENTSOURCING_ENHANCER_ORDER;
    }
}
