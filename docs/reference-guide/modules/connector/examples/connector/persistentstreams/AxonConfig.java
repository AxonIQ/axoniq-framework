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
