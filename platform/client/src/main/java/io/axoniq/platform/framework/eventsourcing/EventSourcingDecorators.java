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
