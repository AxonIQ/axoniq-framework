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

package io.axoniq.platform.framework.messaging.distributed;

import io.axoniq.platform.framework.AxoniqPlatformConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.axoniq.platform.framework.AxoniqPlatformConfigurerEnhancer.PLATFORM_ENHANCER_ORDER;

/**
 * Service-loaded enhancer that decorates the distributed command and query bus configurations so the
 * platform can measure executor usage. Kept free of direct references to {@code axoniq-distributed-messaging}
 * types so the class can be loaded even when that module is absent from the classpath; the decorator wiring
 * lives in {@link DistributedMessagingDecorators} and is only touched after a runtime classpath check.
 */
public class AxoniqPlatformDistributedMessagingConfigurerEnhancer implements ConfigurationEnhancer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AxoniqPlatformDistributedMessagingConfigurerEnhancer.class);
    private static final String DISTRIBUTED_MESSAGING_PROBE_CLASS =
            "io.axoniq.framework.messaging.commandhandling.distributed.DistributedCommandBusConfiguration";

    @Override
    public void enhance(ComponentRegistry registry) {
        if (!registry.hasComponent(AxoniqPlatformConfiguration.class)) {
            return;
        }
        if (!isClasspathAvailable()) {
            LOGGER.debug("axoniq-distributed-messaging not on classpath; skipping distributed messaging decorators.");
            return;
        }
        DistributedMessagingDecorators.apply(registry);
    }

    @Override
    public int order() {
        return PLATFORM_ENHANCER_ORDER;
    }

    private static boolean isClasspathAvailable() {
        try {
            Class.forName(DISTRIBUTED_MESSAGING_PROBE_CLASS, false,
                          AxoniqPlatformDistributedMessagingConfigurerEnhancer.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
