/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package mongo.deadletter;

// tag::queue-configuration[]
import io.axoniq.framework.mongo.deadletter.MongoSequencedDeadLetterQueueConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

import java.time.Duration;

public class PlainQueueConfigurationConfig {

    public void configureDeadLetterQueue(MessagingConfigurer configurer) {
        configurer.componentRegistry(registry -> registry
                .registerComponent(MongoSequencedDeadLetterQueueConfiguration.class,
                                   config -> MongoSequencedDeadLetterQueueConfiguration.DEFAULT
                                           .maxSequences(256)
                                           .maxSequenceSize(256)
                                           .queryPageSize(50)
                                           .claimDuration(Duration.ofMinutes(1))));
    }
}
// end::queue-configuration[]
