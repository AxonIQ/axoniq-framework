/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package deadletterqueue.index.custompolicy;

import deadletterqueue.index.CustomEnqueuePolicy;

import static deadletterqueue.index.support.Handlers.myHandlerComponent;

// tag::configure-custom-enqueue-policy[]
import io.axoniq.framework.messaging.eventhandling.deadletter.DeadLetterQueueConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
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
                                                        .enqueuePolicy(new CustomEnqueuePolicy()))))
        ));
    }
}
// end::configure-custom-enqueue-policy[]
