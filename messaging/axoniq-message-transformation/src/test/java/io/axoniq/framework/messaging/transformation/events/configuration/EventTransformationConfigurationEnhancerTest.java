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

package io.axoniq.framework.messaging.transformation.events.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import io.axoniq.framework.messaging.transformation.events.EventTransformer;
import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import io.axoniq.framework.messaging.transformation.events.TransformingEventStore;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end wiring: the {@link EventTransformationConfigurationEnhancer} is discovered via
 * {@link ServiceLoader} and installs the {@link TransformingEventStore} decorator only when
 * the application has registered an {@link EventTransformerChain}.
 */
final class EventTransformationConfigurationEnhancerTest {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.Sample", "2.0.0");

    @Test
    void enhancerIsDiscoveredByServiceLoader() {
        boolean found = false;
        for (ConfigurationEnhancer enhancer : ServiceLoader.load(ConfigurationEnhancer.class)) {
            if (enhancer instanceof EventTransformationConfigurationEnhancer) {
                found = true;
                break;
            }
        }
        assertThat(found)
                .as("EventTransformationConfigurationEnhancer must be discoverable via ServiceLoader")
                .isTrue();
    }

    @Test
    void noTransformingDecoratorWhenNoChainIsRegistered() {
        EventStore delegate = Mockito.mock(EventStore.class);

        EventStore resolved = EventSourcingConfigurer.create()
                                                     .registerEventStore(config -> delegate)
                                                     .build()
                                                     .getComponent(EventStore.class);

        // Other framework decorators (e.g. InterceptingEventStore) may still wrap; the
        // contract is that OUR decorator is NOT installed when no chain is registered.
        assertThat(resolved)
                .as("absence of a chain MUST NOT install the TransformingEventStore decorator")
                .isNotInstanceOf(TransformingEventStore.class);
    }

    @Test
    void transformingDecoratorWrapsEventStoreWhenChainIsRegistered() {
        EventStore delegate = Mockito.mock(EventStore.class);
        EventTransformer v1ToV2Transformer = EventTransformation.from(V1).to(V2)
                                                                .transform(JsonNode.class, (in, ctx) -> in.deepCopy());
        EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();

        EventStore resolved = EventSourcingConfigurer.create()
                                                     .registerEventStore(config -> delegate)
                                                     .componentRegistry(cr -> cr.registerComponent(EventTransformerChain.class, c -> chain))
                                                     .build()
                                                     .getComponent(EventStore.class);

        assertThat(resolved)
                .as("registering a chain MUST cause the framework to install the transforming decorator")
                .isInstanceOf(TransformingEventStore.class);
    }
}
