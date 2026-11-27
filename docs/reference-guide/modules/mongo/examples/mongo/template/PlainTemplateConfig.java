/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package mongo.template;

// tag::plain-template[]
import com.mongodb.client.MongoClient;
import io.axoniq.framework.mongo.DefaultMongoTemplate;
import io.axoniq.framework.mongo.MongoTemplate;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

public class PlainTemplateConfig {

    public void configureMongoTemplate(MessagingConfigurer configurer, MongoClient mongoClient) {
        MongoTemplate mongoTemplate = DefaultMongoTemplate.builder()
                                                          .mongoDatabase(mongoClient, "axonframework")
                                                          .build();

        configurer.componentRegistry(registry -> registry.registerComponent(MongoTemplate.class,
                                                                            config -> mongoTemplate));
    }
}
// end::plain-template[]
