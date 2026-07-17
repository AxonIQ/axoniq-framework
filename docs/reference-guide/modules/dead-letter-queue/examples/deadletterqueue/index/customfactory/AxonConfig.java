package deadletterqueue.index.customfactory;

import static deadletterqueue.index.support.Handlers.myHandlerComponent;

// tag::custom-factory[]
import io.axoniq.framework.messaging.deadletter.InMemorySequencedDeadLetterQueue;
import io.axoniq.framework.messaging.eventhandling.deadletter.DeadLetterQueueConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;

public class AxonConfig {

    public void configureDeadLetterQueue(MessagingConfigurer configurer) {
        configurer.eventProcessing(ep -> ep.pooledStreaming(ps -> ps
                .processor(
                        EventProcessorModule
                                .pooledStreaming("my-processor")
                                .eventHandlingComponents(components -> components
                                        .declarative("myHandler", cfg -> myHandlerComponent))
                                .customized((cfg, processorConfig) -> processorConfig
                                        .extend(DeadLetterQueueConfiguration.class,
                                                () -> new DeadLetterQueueConfiguration()
                                                        .enabled()
                                                        .factory((name, config) ->
                                                                InMemorySequencedDeadLetterQueue.<EventMessage>builder()
                                                                        .maxSequences(256)
                                                                        .maxSequenceSize(256)
                                                                        .build())))
                )
        ));
    }
}
// end::custom-factory[]
