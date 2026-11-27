/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package mongo.deadletter;

// tag::spring-queue-configuration[]
import io.axoniq.framework.mongo.deadletter.MongoSequencedDeadLetterQueueConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class SpringQueueConfigurationConfig {

    @Bean
    MongoSequencedDeadLetterQueueConfiguration mongoDeadLetterQueueConfiguration() {
        return MongoSequencedDeadLetterQueueConfiguration.DEFAULT
                .maxSequences(256)
                .maxSequenceSize(256)
                .queryPageSize(50)
                .claimDuration(Duration.ofMinutes(1));
    }
}
// end::spring-queue-configuration[]
