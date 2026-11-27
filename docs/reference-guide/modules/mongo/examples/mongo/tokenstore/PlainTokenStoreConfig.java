/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package mongo.tokenstore;

// tag::plain-token-store[]
import com.mongodb.client.MongoClient;
import io.axoniq.framework.mongo.DefaultMongoTemplate;
import io.axoniq.framework.mongo.MongoTemplate;
import io.axoniq.framework.mongo.token.MongoTokenStoreConfiguration;
import io.axoniq.framework.mongo.transaction.ClientSessionTransactionManager;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;

import java.time.Duration;

public class PlainTokenStoreConfig {

    public void configureTokenStore(MessagingConfigurer configurer, MongoClient mongoClient) {
        configurer.componentRegistry(registry -> registry
                // Tells the module where the tracking token collection lives
                .registerComponent(MongoTemplate.class,
                                   config -> DefaultMongoTemplate.builder().mongoDatabase(mongoClient).build())
                // Hands the token store the MongoDB session of the processing context
                .registerComponent(MongoTransactionalExecutorProvider.class,
                                   config -> new MongoTransactionalExecutorProvider(mongoClient))
                // Binds a MongoDB session and transaction to every processing context
                .registerComponent(TransactionManager.class,
                                   config -> new ClientSessionTransactionManager(mongoClient))
                // Optional: changes the settings of the token store
                .registerComponent(MongoTokenStoreConfiguration.class,
                                   config -> MongoTokenStoreConfiguration.DEFAULT.claimTimeout(Duration.ofSeconds(30))
                                                                                 .nodeId("node-1")));
    }
}
// end::plain-token-store[]
