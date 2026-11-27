/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.configuration;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import io.axoniq.framework.mongo.MongoTemplate;
import io.axoniq.framework.mongo.token.MongoTokenStore;
import io.axoniq.framework.mongo.token.MongoTokenStoreConfiguration;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import io.axoniq.framework.mongo.util.MongoMocks;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link MongoConfigurationEnhancer}.
 *
 * @author Steven van Beelen
 */
class MongoConfigurationEnhancerTest {

    private MongoClient mongoClient;
    private MongoCollection<Document> trackingTokensCollection;
    private MongoTemplate mongoTemplate;
    private MongoTransactionalExecutorProvider provider;

    @BeforeEach
    void setUp() {
        mongoClient = MongoMocks.client();
        trackingTokensCollection = MongoMocks.collection();
        mongoTemplate = mock(MongoTemplate.class);
        when(mongoTemplate.trackingTokensCollection()).thenReturn(trackingTokensCollection);
        when(mongoTemplate.deadLetterCollection()).thenReturn(MongoMocks.collection());
        provider = new MongoTransactionalExecutorProvider(mongoClient);
    }

    @Test
    void orderIsMinValuePlusThirty() {
        // given / when / then
        assertThat(new MongoConfigurationEnhancer().order()).isEqualTo(Integer.MIN_VALUE + 30);
    }

    @Nested
    class WithMongoTemplate {

        @Test
        void registersMongoTokenStore() {
            // given
            MessagingConfigurer configurer = MessagingConfigurer.create().componentRegistry(
                    cr -> cr.registerComponent(MongoTemplate.class, c -> mongoTemplate)
                            .registerComponent(MongoTransactionalExecutorProvider.class, c -> provider)
            );

            // when
            AxonConfiguration configuration = configurer.build();

            // then
            assertThat(configuration.getComponent(TokenStore.class)).isInstanceOf(MongoTokenStore.class);
        }

        @Test
        void registersDefaultMongoTokenStoreConfiguration() {
            // given
            MessagingConfigurer configurer = MessagingConfigurer.create().componentRegistry(
                    cr -> cr.registerComponent(MongoTemplate.class, c -> mongoTemplate)
                            .registerComponent(MongoTransactionalExecutorProvider.class, c -> provider)
            );

            // when
            AxonConfiguration configuration = configurer.build();

            // then
            assertThat(configuration.getComponent(MongoTokenStoreConfiguration.class))
                    .isEqualTo(MongoTokenStoreConfiguration.DEFAULT);
        }

        @Test
        void constructsMongoTokenStoreWithExplicitlyRegisteredConfiguration() {
            // given
            MessagingConfigurer configurer = MessagingConfigurer.create().componentRegistry(
                    cr -> cr.registerComponent(MongoTemplate.class, c -> mongoTemplate)
                            .registerComponent(MongoTransactionalExecutorProvider.class, c -> provider)
                            .registerComponent(MongoTokenStoreConfiguration.class,
                                               c -> MongoTokenStoreConfiguration.DEFAULT.ensureIndexes(false))
            );

            // when
            AxonConfiguration configuration = configurer.build();
            configuration.getComponent(TokenStore.class);

            // then
            assertThat(configuration.getComponent(MongoTokenStoreConfiguration.class).ensureIndexes()).isFalse();
            verify(trackingTokensCollection, never()).createIndex(any(ClientSession.class),
                                                                  any(Bson.class),
                                                                  any(IndexOptions.class));
        }

        @Test
        void doesNotReplaceExplicitlyRegisteredTokenStore() {
            // given
            TokenStore explicitTokenStore = new InMemoryTokenStore() {
            };
            MessagingConfigurer configurer = MessagingConfigurer.create().componentRegistry(cr -> cr
                    .registerComponent(MongoTemplate.class, c -> mongoTemplate)
                    .registerComponent(MongoTransactionalExecutorProvider.class, c -> provider)
                    .registerComponent(TokenStore.class, c -> explicitTokenStore)
            );

            // when
            AxonConfiguration configuration = configurer.build();

            // then
            assertThat(configuration.getComponent(TokenStore.class)).isSameAs(explicitTokenStore);
        }
    }

    @Nested
    class WithoutMongoTemplate {

        @Test
        void aMongoClientAloneDoesNotActivateTheEnhancer() {
            // given
            MessagingConfigurer configurer = MessagingConfigurer.create().componentRegistry(
                    cr -> cr.registerComponent(MongoClient.class, c -> mongoClient)
                            .registerComponent(MongoTransactionalExecutorProvider.class, c -> provider)
            );

            // when
            AxonConfiguration configuration = configurer.build();

            // then
            assertThat(configuration.getComponent(TokenStore.class)).isInstanceOf(InMemoryTokenStore.class);
            assertThat(configuration.getOptionalComponent(MongoTemplate.class)).isEmpty();
        }

        @Test
        void keepsTheDefaultTokenStore() {
            // given
            MessagingConfigurer configurer = MessagingConfigurer.create();

            // when
            AxonConfiguration configuration = configurer.build();

            // then
            assertThat(configuration.getComponent(TokenStore.class)).isInstanceOf(InMemoryTokenStore.class);
        }
    }
}
