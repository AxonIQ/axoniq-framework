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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.axoniq.platform.framework.AxoniqPlatformConfigurerEnhancer.PLATFORM_ENHANCER_ORDER;

/**
 * Service-loaded enhancer that decorates the Axon Framework {@code EventStorageEngine}. Kept free of direct
 * references to {@code axon-eventsourcing} types so the class can be loaded even when event sourcing is
 * absent from the classpath; the decorator wiring lives in {@link EventSourcingDecorators} and is only
 * touched after a runtime classpath check.
 */
public class AxoniqPlatformEventsourcingConfigurerEnhancer implements ConfigurationEnhancer {

    /**
     * This enhancer should be configured after the platform enhancer, to have the HandlerMetricRegistry component
     * available.
     */
    public static final int EVENTSOURCING_ENHANCER_ORDER = PLATFORM_ENHANCER_ORDER + 1;

    private static final Logger LOGGER = LoggerFactory.getLogger(AxoniqPlatformEventsourcingConfigurerEnhancer.class);
    private static final String EVENTSOURCING_PROBE_CLASS = "org.axonframework.eventsourcing.eventstore.EventStorageEngine";

    @Override
    public void enhance(ComponentRegistry registry) {
        if (!registry.hasComponent(HandlerMetricsRegistry.class)) {
            return;
        }
        if (!isClasspathAvailable()) {
            LOGGER.debug("axon-eventsourcing not on classpath; skipping event store decorator.");
            return;
        }
        EventSourcingDecorators.apply(registry);
    }

    @Override
    public int order() {
        return EVENTSOURCING_ENHANCER_ORDER;
    }

    private static boolean isClasspathAvailable() {
        try {
            Class.forName(EVENTSOURCING_PROBE_CLASS, false,
                          AxoniqPlatformEventsourcingConfigurerEnhancer.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
