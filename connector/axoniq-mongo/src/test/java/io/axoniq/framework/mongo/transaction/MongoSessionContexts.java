/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.transaction;

import com.mongodb.client.MongoClient;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;

/**
 * Utility class creating {@link ProcessingContext ProcessingContexts} that have a MongoDB session bound to them, for
 * testing.
 *
 * @author Steven van Beelen
 */
public final class MongoSessionContexts {

    private MongoSessionContexts() {
        // Test utility class
    }

    /**
     * Constructs a {@link ProcessingContext} with a session of the given {@code mongoClient} bound to it. The session
     * has no transaction, so this works against a standalone MongoDB instance too.
     *
     * @param mongoClient the {@link MongoClient} to start the session on
     * @return a {@link ProcessingContext} with a session bound to it
     */
    public static ProcessingContext withSession(MongoClient mongoClient) {
        StubProcessingContext context = new StubProcessingContext();
        ClientSessionExecutor executor = new ClientSessionExecutor(mongoClient::startSession);
        context.putResource(MongoTransactionalExecutorProvider.SUPPLIER_KEY, () -> executor);
        return context;
    }
}
