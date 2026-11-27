/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.mongo;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.springframework.data.mongodb.MongoDatabaseFactory;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Utility class creating a {@link MongoDatabaseFactory} that does not connect to MongoDB, for tests that validate
 * wiring and naming. Every database, collection and session it hands out is a mock, and the namespace of a mocked
 * collection reflects the database and collection that were requested. Tests that depend on the behavior of MongoDB
 * itself are integration tests, which run against a MongoDB container instead.
 *
 * @author Steven van Beelen
 */
public final class MockMongoDatabaseFactory {

    private MockMongoDatabaseFactory() {
        // Test utility class
    }

    /**
     * Constructs a {@link MongoDatabaseFactory} whose default database is named {@code defaultDatabase}.
     *
     * @param defaultDatabase the name of the default database of the factory
     * @return a {@link MongoDatabaseFactory} that does not connect to MongoDB
     */
    public static MongoDatabaseFactory create(String defaultDatabase) {
        MongoDatabase defaultDb = database(defaultDatabase);
        MongoDatabaseFactory factory = mock(MongoDatabaseFactory.class);
        when(factory.getMongoDatabase()).thenReturn(defaultDb);
        when(factory.getMongoDatabase(anyString())).thenAnswer(invocation -> database(invocation.getArgument(0)));
        when(factory.getSession(any(ClientSessionOptions.class)))
                .thenAnswer(invocation -> mock(ClientSession.class));
        return factory;
    }

    private static MongoDatabase database(String name) {
        MongoDatabase database = mock(MongoDatabase.class);
        when(database.getName()).thenReturn(name);
        when(database.getCollection(anyString())).thenAnswer(invocation -> collection(name, invocation.getArgument(0)));
        return database;
    }

    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> collection(String databaseName, String collectionName) {
        MongoCollection<Document> collection = mock(MongoCollection.class);
        when(collection.getNamespace()).thenReturn(new MongoNamespace(databaseName, collectionName));
        return collection;
    }
}
