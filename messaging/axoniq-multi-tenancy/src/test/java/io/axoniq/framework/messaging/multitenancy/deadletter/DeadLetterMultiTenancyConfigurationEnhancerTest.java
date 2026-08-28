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

import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.api.RecordingAxonServerConnectionManager;
import io.axoniq.framework.messaging.eventhandling.deadletter.DeadLetterQueueConfiguration;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.AsyncInMemoryStreamableEventSource;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeadLetterMultiTenancyConfigurationEnhancerTest {

    @Test
    void configuresTenantRoutingDeadLetterQueueFactoryWhenTheDeadLetterQueueModuleIsAvailable() {
        // given
        DeadLetterQueueConfiguration dlqConfig = new DeadLetterQueueConfiguration().enabled();
        var component = SimpleEventHandlingComponent.create("handler");
        component.subscribe(new QualifiedName(String.class), (event, context) -> MessageStream.empty());
        var module = EventProcessorModule.pooledStreaming("processor")
                                         .eventHandlingComponents(components -> components.declarative(
                                                 "handler", config -> component
                                         ))
                                         .customized((config, processorConfig) -> processorConfig
                                                 .eventSource(new AsyncInMemoryStreamableEventSource())
                                                 .extend(DeadLetterQueueConfiguration.class, () -> dlqConfig));

        // when
        var configuration = MessagingConfigurer.create()
                                              .componentRegistry(registry -> registry
                                                      .registerComponent(AxonServerConnectionManager.class,
                                                                         config -> new RecordingAxonServerConnectionManager())
                                                      .registerComponent(TenantProvider.class,
                                                                         config -> new StubTenantProvider()))
                                              .componentRegistry(registry -> registry.registerComponent(
                                                      TenantAwareSequencedDeadLetterQueueFactory.class,
                                                      config -> (tenant, processorName, ignored) -> null
                                              ))
                                              .eventProcessing(eventProcessing -> eventProcessing.pooledStreaming(
                                                      pooledStreaming -> pooledStreaming.processor(module)
                                              ))
                                              .build();

        // then
        var processorConfig = configuration.getModuleConfiguration("EventProcessor[processor]")
                                           .flatMap(moduleConfig -> moduleConfig.getOptionalComponent(
                                                   PooledStreamingEventProcessorConfiguration.class
                                           ));
        assertThat(processorConfig).isPresent();
        assertThat(configuration.hasComponent(TenantRoutingSequencedDeadLetterQueueRegistry.class)).isTrue();
        assertThat(processorConfig.orElseThrow()
                                  .extension(DeadLetterQueueConfiguration.class)
                                  .factory()).isInstanceOf(TenantRoutingSequencedDeadLetterQueueFactory.class);
        assertThat(dlqConfig.factory()).isInstanceOf(TenantRoutingSequencedDeadLetterQueueFactory.class);
    }

    @Test
    void detectsWhenTheDeadLetterQueueModuleIsUnavailable() {
        ClassLoader classLoaderWithoutDeadLetterQueue = new ClassLoader(null) {
        };

        assertThat(DeadLetterMultiTenancyConfigurationEnhancer.isDeadLetterQueuePresent(
                classLoaderWithoutDeadLetterQueue
        )).isFalse();
    }

    @Test
    void detectsWhenTheDeadLetterQueueModuleIsAvailable() {
        assertThat(DeadLetterMultiTenancyConfigurationEnhancer.isDeadLetterQueuePresent(
                getClass().getClassLoader()
        )).isTrue();
    }
}
