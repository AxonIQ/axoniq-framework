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

// tag::declarative-persistent-stream-config[]
import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSource;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSourceFactory;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamSequencingPolicy;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.configuration.EventHandlingComponentsConfigurer;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessorsConfigurer;

import java.util.Collections;

public class AxonConfig {

    public void configureEventProcessing(MessagingConfigurer messaging) {
        messaging.eventProcessing(
                eventProcessing -> eventProcessing.subscribing(
                    this::configureSubscribingProcessor
        ));
    }

    private SubscribingEventProcessorsConfigurer configureSubscribingProcessor(
            SubscribingEventProcessorsConfigurer subscribingConfigurer
    ) {
        return subscribingConfigurer.processor(
                "example-processor",
                config -> config.eventHandlingComponents(this::configureHandlingComponent)
                        .customized((c, subscribingConfig) -> subscribingConfig.eventSource(
                                buildPersistentStreamSource(c)
                        ))
        );
    }

    private PersistentStreamEventSource buildPersistentStreamSource(Configuration c) {
        String streamName = "example-persistent-stream";
        int segmentCount = 4;
        int batchSize = 1024;

        PersistentStreamProperties properties = new PersistentStreamProperties(
                streamName,
                segmentCount,
                PersistentStreamSequencingPolicy.SEQUENTIAL_POLICY,
                Collections.emptyList(),
                "HEAD",
                null  // no server-side filter
        );

        return PersistentStreamEventSourceFactory.defaultFactory()
                .build(streamName,
                        properties,
                        PersistentStreamScheduledExecutorBuilder.defaultFactory().build(segmentCount, streamName),
                        batchSize,
                        c);
    }

    private EventHandlingComponentsConfigurer.AdditionalComponentPhase configureHandlingComponent(
            EventHandlingComponentsConfigurer.RequiredComponentPhase componentConfigurer
    ) {
        return componentConfigurer.autodetected(c -> new AnnotatedEventHandlingClass());
    }
}
// end::declarative-persistent-stream-config[]
