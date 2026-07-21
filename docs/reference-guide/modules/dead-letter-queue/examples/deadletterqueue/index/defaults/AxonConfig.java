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
