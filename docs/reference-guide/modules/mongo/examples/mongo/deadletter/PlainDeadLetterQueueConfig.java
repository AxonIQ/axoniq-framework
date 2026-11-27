/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package mongo.deadletter;

import static deadletterqueue.index.support.Handlers.myHandlerComponent;

// tag::mongo-dead-letter-queue[]
import io.axoniq.framework.messaging.eventhandling.deadletter.DeadLetterQueueConfiguration;
import io.axoniq.framework.mongo.MongoTemplate;
import io.axoniq.framework.mongo.deadletter.MongoSequencedDeadLetterQueueConfiguration;
import io.axoniq.framework.mongo.deadletter.MongoSequencedDeadLetterQueueFactory;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

public class PlainDeadLetterQueueConfig {

    public void configureDeadLetterQueue(MessagingConfigurer configurer,
                                         MongoTemplate mongoTemplate,
                                         MongoTransactionalExecutorProvider provider,
                                         EventConverter eventConverter,
                                         GeneralConverter converter,
                                         MongoSequencedDeadLetterQueueConfiguration queueConfiguration) {
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
                                                        .factory(new MongoSequencedDeadLetterQueueFactory(
                                                                mongoTemplate.deadLetterCollection(),
                                                                provider,
                                                                eventConverter,
                                                                converter,
                                                                queueConfiguration))))
                )
        ));
    }
}
// end::mongo-dead-letter-queue[]
