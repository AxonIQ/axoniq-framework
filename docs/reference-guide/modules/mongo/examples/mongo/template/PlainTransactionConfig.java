/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package mongo.template;

// tag::plain-transaction[]
import com.mongodb.client.MongoClient;
import io.axoniq.framework.mongo.transaction.ClientSessionTransactionManager;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;

public class PlainTransactionConfig {

    public void configureTransactions(MessagingConfigurer configurer, MongoClient mongoClient) {
        configurer.componentRegistry(registry -> registry
                // Binds a MongoDB session and transaction to every processing context
                .registerComponent(TransactionManager.class,
                                   config -> new ClientSessionTransactionManager(mongoClient))
                // Hands the MongoDB components the session of the processing context they are invoked with
                .registerComponent(MongoTransactionalExecutorProvider.class,
                                   config -> new MongoTransactionalExecutorProvider(mongoClient)));
    }
}
// end::plain-transaction[]
