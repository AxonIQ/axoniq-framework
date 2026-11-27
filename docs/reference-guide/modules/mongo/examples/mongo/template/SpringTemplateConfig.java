/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package mongo.template;

// tag::spring-template[]
import io.axoniq.framework.mongo.MongoTemplate;
import io.axoniq.framework.springboot.mongo.SpringMongoTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;

@Configuration
public class SpringTemplateConfig {

    @Bean
    MongoTemplate axonMongoTemplate(MongoDatabaseFactory databaseFactory) {
        return SpringMongoTemplate.builder()
                                  .factory(databaseFactory)
                                  .databaseName("axonframework")
                                  .trackingTokensCollectionName("my-tracking-tokens")
                                  .deadLetterCollectionName("my-dead-letters")
                                  .build();
    }
}
// end::spring-template[]
