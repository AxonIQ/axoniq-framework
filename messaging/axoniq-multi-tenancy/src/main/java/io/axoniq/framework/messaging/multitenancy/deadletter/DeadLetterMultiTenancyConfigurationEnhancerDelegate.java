/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.deadletter;

import io.axoniq.framework.messaging.eventhandling.deadletter.DeadLetterQueueConfiguration;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;

import static org.axonframework.common.configuration.DecoratorDefinition.forType;

/**
 * Configures tenant-aware dead-letter queues when the optional dead-letter module is available.
 * <p>
 * Kept in a separate delegate to avoid a hard dependency on the optional dead-letter module in the SPI provider.
 * The {@link DeadLetterMultiTenancyConfigurationEnhancer} loads this class and invokes {@link #enhance(ComponentRegistry)}
 * reflectively only when the dead-letter module is available.
 *
 * @author Jan Galinski
 * @since 5.4.0
 */
@SuppressWarnings("unused")
@Internal
final class DeadLetterMultiTenancyConfigurationEnhancerDelegate {

    private static final String EXP_MSG = "A TenantAwareSequencedDeadLetterQueueFactory must be configured when multi-tenancy and the dead-letter queue are enabled.";

    static void enhance(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(TenantRoutingSequencedDeadLetterQueueRegistry.class,
                                               configuration -> new TenantRoutingSequencedDeadLetterQueueRegistry(),
                                               SearchScope.ALL);
        componentRegistry.registerDecorator(
                forType(PooledStreamingEventProcessorConfiguration.class).with(
                        (config, name, processorConfiguration) -> {
                            DeadLetterQueueConfiguration dlqConfig =
                                    processorConfiguration.extension(DeadLetterQueueConfiguration.class);
                            if (dlqConfig != null && dlqConfig.isEnabled()) {
                                TenantAwareSequencedDeadLetterQueueFactory tenantFactory =
                                        config.getOptionalComponent(TenantAwareSequencedDeadLetterQueueFactory.class)
                                              .orElseThrow(() -> new AxonConfigurationException(EXP_MSG));

                                dlqConfig.factory(new TenantRoutingSequencedDeadLetterQueueFactory(
                                        tenantFactory,
                                        config.getComponent(TenantRoutingSequencedDeadLetterQueueRegistry.class)
                                ));
                            }
                            return processorConfiguration;
                        }
                )
        );
    }

    private DeadLetterMultiTenancyConfigurationEnhancerDelegate() {
        // utility class
    }
}
