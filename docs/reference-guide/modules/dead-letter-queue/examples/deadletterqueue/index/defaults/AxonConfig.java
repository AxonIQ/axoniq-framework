/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package deadletterqueue.index.defaults;

import static deadletterqueue.index.support.Handlers.myHandlerComponent;

// tag::configure-dead-letter-queue-defaults[]
import io.axoniq.framework.messaging.eventhandling.deadletter.DeadLetterQueueConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;

public class AxonConfig {

    public void configureDeadLetterQueue(MessagingConfigurer configurer) {
        configurer.eventProcessing(ep -> ep.pooledStreaming(ps -> ps
                // Enable DLQ for all pooled streaming processors by default
                .defaults(d -> d.extend(DeadLetterQueueConfiguration.class,
                        () -> new DeadLetterQueueConfiguration()
                                .enabled()
                                .cacheMaxSize(2048)))
                // This processor inherits the defaults
                .processor(EventProcessorModule
                        .pooledStreaming("processor-with-dlq")
                        .eventHandlingComponents(components -> components
                                .declarative("myHandler", cfg -> myHandlerComponent))
                        .notCustomized())
                // This processor explicitly disables DLQ
                .processor(EventProcessorModule
                        .pooledStreaming("processor-without-dlq")
                        .eventHandlingComponents(components -> components
                                .declarative("myHandler", cfg -> myHandlerComponent))
                        .customized((cfg, c) -> c
                                .extend(DeadLetterQueueConfiguration.class,
                                        () -> new DeadLetterQueueConfiguration().disabled())))
        ));
    }
}
// end::configure-dead-letter-queue-defaults[]
