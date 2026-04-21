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
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;

/**
 * Holder of the actual decorator registration against {@code axon-eventsourcing} types. Kept separate from
 * {@link AxoniqPlatformEventsourcingConfigurerEnhancer} so that the enhancer class can be loaded even when
 * {@code axon-eventsourcing} is not on the classpath — this class is only touched after a {@code Class.forName}
 * probe confirms the module is present.
 */
final class EventSourcingDecorators {

    private EventSourcingDecorators() {
    }

    static void apply(ComponentRegistry registry) {
        registry.registerDecorator(
                DecoratorDefinition.forType(EventStorageEngine.class)
                                   .with((c, name, delegate) -> {
                                       HandlerMetricsRegistry metricsRegistry = c.getComponent(HandlerMetricsRegistry.class);
                                       return new AxoniqPlatformEventStorageEngine(delegate, metricsRegistry);
                                   }).order(Integer.MAX_VALUE));
    }
}
