/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package mongo.template;

// tag::custom-collection-names[]
import com.mongodb.client.MongoClient;
import io.axoniq.framework.mongo.DefaultMongoTemplate;
import io.axoniq.framework.mongo.MongoTemplate;

public class CustomCollectionNamesConfig {

    public MongoTemplate mongoTemplate(MongoClient mongoClient) {
        return DefaultMongoTemplate.builder()
                                   .mongoDatabase(mongoClient, "axonframework")
                                   .trackingTokensCollectionName("my-tracking-tokens")
                                   .deadLetterCollectionName("my-dead-letters")
                                   .build();
    }
}
// end::custom-collection-names[]
