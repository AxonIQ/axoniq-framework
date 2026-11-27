/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link DefaultMongoTemplate}.
 */
class DefaultMongoTemplateTest {

    private MongoClient mockMongo;
    private MongoDatabase mockDb;

    @BeforeEach
    void createFixtures() {
        mockMongo = mock(MongoClient.class);
        mockDb = mock(MongoDatabase.class);
        @SuppressWarnings("unchecked")
        MongoCollection<Document> mockCollection = mock(MongoCollection.class);

        when(mockMongo.getDatabase(anyString())).thenReturn(mockDb);
        when(mockDb.getCollection(anyString())).thenReturn(mockCollection);
    }

    @Test
    void trackingTokenCollectionUsesDefaultNames() {
        // given
        DefaultMongoTemplate testSubject = DefaultMongoTemplate.builder().mongoDatabase(mockMongo).build();

        // when
        testSubject.trackingTokensCollection();

        // then
        verify(mockMongo).getDatabase("axonframework");
        verify(mockDb).getCollection("trackingtokens");
    }

    @Test
    void trackingTokenCollectionUsesCustomName() {
        // given
        DefaultMongoTemplate testSubject = DefaultMongoTemplate.builder()
                                                               .mongoDatabase(mockMongo, "customDatabaseName")
                                                               .build()
                                                               .withTrackingTokenCollection("customCollectionName");

        // when
        testSubject.trackingTokensCollection();

        // then
        verify(mockMongo).getDatabase("customDatabaseName");
        verify(mockDb).getCollection("customCollectionName");
    }

    @Test
    void deadLetterCollectionUsesCustomNameWhenSet() {
        // given
        DefaultMongoTemplate testSubject = DefaultMongoTemplate.builder()
                                                               .mongoDatabase(mockMongo, "customDatabaseName")
                                                               .build()
                                                               .withDeadLetterCollection("customCollectionName");

        // when
        testSubject.deadLetterCollection();

        // then
        verify(mockMongo).getDatabase("customDatabaseName");
        verify(mockDb).getCollection("customCollectionName");
    }
}
