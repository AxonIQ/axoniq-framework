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

package connector.persistentstreams;

// tag::event-processor-definition-bean[]
import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSourceFactory;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamSequencingPolicy;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.extension.spring.config.EventHandlerSelector;
import org.axonframework.extension.spring.config.EventProcessorDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
class EventProcessorConfig {

    private static final String STREAM_NAME = "example-persistent-stream";
    private static final int SEGMENT_COUNT = 4;

    @Bean(destroyMethod = "shutdown") // <1>
    public ScheduledExecutorService exampleStreamExecutor(
            PersistentStreamScheduledExecutorBuilder executorBuilder) {
        return executorBuilder.build(SEGMENT_COUNT, STREAM_NAME);
    }

    @Bean
    public EventProcessorDefinition exampleProcessorDefinition(
            AxonConfiguration configuration,
            PersistentStreamEventSourceFactory eventSourceFactory,
            ScheduledExecutorService exampleStreamExecutor) {
        int batchSize = 1024;

        PersistentStreamProperties properties = new PersistentStreamProperties(
                STREAM_NAME,
                SEGMENT_COUNT,
                PersistentStreamSequencingPolicy.SEQUENTIAL_PER_AGGREGATE_POLICY,
                Collections.emptyList(),
                "HEAD",
                null  // no server-side filter
        );

        return EventProcessorDefinition.subscribing("example-processor")
                .assigningHandlers(EventHandlerSelector.matchesNamespaceOnType("orders"))
                // Build the event source inside the lambda: it runs once the Axon configuration
                // is fully initialized. Querying the AxonConfiguration during bean construction
                // fails the application start-up.
                .customized(config -> config.eventSource(
                        eventSourceFactory.build(STREAM_NAME,
                                                 properties,
                                                 exampleStreamExecutor,
                                                 batchSize,
                                                 configuration))); // <2>
    }
}
// end::event-processor-definition-bean[]
