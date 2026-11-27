/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package mongo.tokenstore;

// tag::projection-handler[]
import com.mongodb.client.MongoCollection;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.bson.Document;

import java.util.concurrent.CompletableFuture;

class OrderProjection {

    private final MongoTransactionalExecutorProvider executorProvider;
    private final MongoCollection<Document> orders;

    OrderProjection(MongoTransactionalExecutorProvider executorProvider, MongoCollection<Document> orders) {
        this.executorProvider = executorProvider;
        this.orders = orders;
    }

    // OrderPlaced is your application-defined event class.
    @EventHandler
    public CompletableFuture<Void> on(OrderPlaced event, ProcessingContext context) {
        // Writing through the session of the processing context makes the projection update part of the
        // transaction that also stores the tracking token.
        return executorProvider.getTransactionalExecutor(context)
                               .accept(session -> orders.insertOne(session, new Document("_id", event.orderId())));
    }
}
// end::projection-handler[]

record OrderPlaced(String orderId) {

}
