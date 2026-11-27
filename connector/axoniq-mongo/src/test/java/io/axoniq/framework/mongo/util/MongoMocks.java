/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.util;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Utility class creating mocked MongoDB components, for tests that validate wiring and argument handling without a
 * MongoDB instance. Tests that depend on the behavior of MongoDB itself are integration tests, which run against a
 * MongoDB container instead.
 *
 * @author Steven van Beelen
 */
public final class MongoMocks {

    private MongoMocks() {
        // Test utility class
    }

    /**
     * Constructs a mocked {@link MongoClient}. Every {@link MongoClient#startSession()} returns a new mocked
     * {@link ClientSession}, and every database and collection that is requested is a mock.
     *
     * @return a mocked {@link MongoClient}
     */
    @SuppressWarnings("unchecked")
    public static MongoClient client() {
        MongoClient client = mock(MongoClient.class);
        MongoDatabase database = mock(MongoDatabase.class);
        when(client.getDatabase(anyString())).thenReturn(database);
        when(database.getCollection(anyString())).thenAnswer(invocation -> mock(MongoCollection.class));
        when(client.startSession()).thenAnswer(invocation -> mock(ClientSession.class));
        return client;
    }

    /**
     * Constructs a mocked {@link MongoCollection} of {@link Document Documents}.
     *
     * @return a mocked {@link MongoCollection}
     */
    @SuppressWarnings("unchecked")
    public static MongoCollection<Document> collection() {
        return mock(MongoCollection.class);
    }
}
