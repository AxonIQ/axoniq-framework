/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.messaging.multitenancy.deadletter;

import io.axoniq.framework.messaging.eventhandling.deadletter.DeadLetterQueueConfiguration;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationDefaults;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;

import static org.axonframework.common.configuration.DecoratorDefinition.forType;

/**
 * Makes an enabled dead-letter queue configuration tenant-aware by routing its configured queue factory per tenant.
 *
 * @author Jan Galinski
 * @since 5.4.0
 */
@Internal
public class DeadLetterMultiTenancyConfigurationEnhancer implements ConfigurationEnhancer {

    private static final String DEAD_LETTER_QUEUE_CONFIGURATION =
            "io.axoniq.framework.messaging.eventhandling.deadletter.DeadLetterQueueConfiguration";

    /**
     * The order at which dead-letter queue support is configured after the general and Axon Server multi-tenancy
     * components.
     */
    public static final int ENHANCER_ORDER = MultiTenancyConfigurationDefaults.ENHANCER_ORDER + 4;

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }

    private static void registerDeadLetterQueueDecorator(ComponentRegistry componentRegistry) {
        componentRegistry.registerDecorator(forType(PooledStreamingEventProcessorConfiguration.class)
                                                    .with((configuration, name, processorConfiguration) -> {
                                                        DeadLetterQueueConfiguration dlqConfig =
                                                                processorConfiguration.extension(
                                                                        DeadLetterQueueConfiguration.class);
                                                        if (dlqConfig != null && dlqConfig.isEnabled()) {
                                                            dlqConfig.factory(new TenantRoutingSequencedDeadLetterQueueFactory(
                                                                    dlqConfig.factory(),
                                                                    configuration.getComponent(
                                                                            TenantRoutingSequencedDeadLetterQueueRegistry.class
                                                                    )
                                                            ));
                                                        }
                                                        return processorConfiguration;
                                                    }));
    }

    static boolean isDeadLetterQueuePresent(ClassLoader classLoader) {
        try {
            Class.forName(DEAD_LETTER_QUEUE_CONFIGURATION, false, classLoader);
            return true;
        } catch (ClassNotFoundException | LinkageError ignored) {
            return false;
        }
    }

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        if (!isDeadLetterQueuePresent(getClass().getClassLoader())) {
            return;
        }
        componentRegistry.registerIfNotPresent(TenantRoutingSequencedDeadLetterQueueRegistry.class,
                                               configuration -> new TenantRoutingSequencedDeadLetterQueueRegistry(),
                                               SearchScope.ALL);
        registerDeadLetterQueueDecorator(componentRegistry);
    }
}
