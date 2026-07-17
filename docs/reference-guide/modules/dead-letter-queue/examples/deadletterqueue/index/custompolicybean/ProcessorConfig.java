package deadletterqueue.index.custompolicybean;

import deadletterqueue.index.CustomEnqueuePolicy;

// tag::event-processor-definition-policy[]
import io.axoniq.framework.messaging.eventhandling.deadletter.DeadLetterQueueConfiguration;
import org.axonframework.extension.spring.config.EventProcessorDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProcessorConfig {

    @Bean
    EventProcessorDefinition myProcessor() {
        return EventProcessorDefinition.pooledStreaming("my-processor")
                .assigningHandlers(descriptor ->
                        descriptor.beanName().startsWith("my"))
                .customized(config -> config
                        .extend(DeadLetterQueueConfiguration.class,
                                () -> new DeadLetterQueueConfiguration()
                                        .enabled()
                                        .enqueuePolicy(new CustomEnqueuePolicy())));
    }
}
// end::event-processor-definition-policy[]
