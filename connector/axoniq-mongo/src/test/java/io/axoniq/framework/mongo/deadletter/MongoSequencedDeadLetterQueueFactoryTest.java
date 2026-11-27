/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import io.axoniq.framework.mongo.util.MongoMocks;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.bson.Document;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link MongoSequencedDeadLetterQueueFactory}.
 *
 * @author Steven van Beelen
 */
class MongoSequencedDeadLetterQueueFactoryTest {

    private final JacksonConverter converter = new JacksonConverter();
    private final EventConverter eventConverter = new DelegatingEventConverter(converter);

    private MongoCollection<Document> deadLetterCollection;
    private TransactionalExecutorProvider<ClientSession> provider;
    private AxonConfiguration configuration;

    @BeforeEach
    void setUp() {
        deadLetterCollection = MongoMocks.collection();
        provider = new MongoTransactionalExecutorProvider(MongoMocks.client());
        configuration = MessagingConfigurer.create().build();
    }

    private MongoSequencedDeadLetterQueueFactory factory(MongoCollection<Document> collection,
                                                         TransactionalExecutorProvider<ClientSession> provider,
                                                         EventConverter eventConverter,
                                                         Converter genericConverter,
                                                         MongoSequencedDeadLetterQueueConfiguration queueConfig) {
        return new MongoSequencedDeadLetterQueueFactory(collection,
                                                        provider,
                                                        eventConverter,
                                                        genericConverter,
                                                        queueConfig);
    }

    @Test
    void createReturnsMongoQueueForTheGivenProcessingGroup() {
        // given
        MongoSequencedDeadLetterQueueFactory testSubject = factory(deadLetterCollection,
                                                                   provider,
                                                                   eventConverter,
                                                                   converter,
                                                                   MongoSequencedDeadLetterQueueConfiguration.DEFAULT);

        // when
        SequencedDeadLetterQueue<EventMessage> result = testSubject.create("DeadLetterQueue[p][c]", configuration);

        // then
        assertThat(result).isInstanceOf(MongoSequencedDeadLetterQueue.class);
    }

    @Nested
    class Construction {

        private final MongoSequencedDeadLetterQueueConfiguration queueConfiguration =
                MongoSequencedDeadLetterQueueConfiguration.DEFAULT;

        @Test
        void rejectsNullDeadLetterCollection() {
            assertThatThrownBy(() -> factory(null, provider, eventConverter, converter, queueConfiguration))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsNullTransactionalExecutorProvider() {
            assertThatThrownBy(() -> factory(deadLetterCollection, null, eventConverter, converter, queueConfiguration))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsNullEventConverter() {
            assertThatThrownBy(() -> factory(deadLetterCollection, provider, null, converter, queueConfiguration))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsNullGenericConverter() {
            assertThatThrownBy(() -> factory(deadLetterCollection, provider, eventConverter, null, queueConfiguration))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsNullQueueConfiguration() {
            assertThatThrownBy(() -> factory(deadLetterCollection, provider, eventConverter, converter, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}
