/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.token;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import io.axoniq.framework.mongo.util.MongoMocks;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Test class validating whether a {@link MongoTokenStore} creates indexes, depending on its configuration.
 *
 * @author erikrz
 */
class MongoTokenStoreSkipIndexTest {

    private MongoCollection<Document> trackingTokensCollection;
    private MongoTransactionalExecutorProvider provider;

    @BeforeEach
    void setUp() {
        trackingTokensCollection = MongoMocks.collection();
        provider = new MongoTransactionalExecutorProvider(MongoMocks.client());
    }

    @Test
    void createsTheIndexWhenEnsureIndexesIsEnabled() {
        // given / when
        new MongoTokenStore(trackingTokensCollection,
                            provider,
                            new JacksonConverter(),
                            MongoTokenStoreConfiguration.DEFAULT);

        // then
        verify(trackingTokensCollection).createIndex(any(ClientSession.class), any(Bson.class), any(IndexOptions.class));
    }

    @Test
    void skipsIndexCreationWhenEnsureIndexesIsDisabled() {
        // given / when
        new MongoTokenStore(trackingTokensCollection,
                            provider,
                            new JacksonConverter(),
                            MongoTokenStoreConfiguration.DEFAULT.ensureIndexes(false));

        // then
        verify(trackingTokensCollection, never()).createIndex(any(ClientSession.class),
                                                              any(Bson.class),
                                                              any(IndexOptions.class));
    }
}
