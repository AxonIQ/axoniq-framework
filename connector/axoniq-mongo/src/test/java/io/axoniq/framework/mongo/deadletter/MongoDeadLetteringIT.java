/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.framework.messaging.eventhandling.deadletter.AbstractDeadLetteringEventIT;
import io.axoniq.framework.mongo.DefaultMongoTemplate;
import io.axoniq.framework.mongo.MongoTemplate;
import io.axoniq.framework.mongo.transaction.ClientSessionTransactionManager;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import io.axoniq.framework.mongo.util.MongoTestImage;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * An implementation of the {@link AbstractDeadLetteringEventIT} validating the {@link MongoSequencedDeadLetterQueue}
 * with an event processor and {@code DeadLetteringEventHandlingComponent}.
 * <p>
 * The processing contexts get their MongoDB session and transaction from the {@link ClientSessionTransactionManager},
 * so the queue operations run in a MongoDB transaction exactly as they do in production. This requires a replica set.
 */
@Testcontainers
class MongoDeadLetteringIT extends AbstractDeadLetteringEventIT {

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer(MongoTestImage.NAME).withReplicaSet();
    private static final String DATABASE = "dead-lettering-it";
    private static final int MAX_SEQUENCES_AND_SEQUENCE_SIZE = 128;

    private final JacksonConverter jacksonConverter = new JacksonConverter();
    private final MongoClient mongoClient = MongoClients.create(MONGO_CONTAINER.getReplicaSetUrl(DATABASE));

    @Override
    protected SequencedDeadLetterQueue<EventMessage> buildDeadLetterQueue() {
        MongoTemplate mongoTemplate = DefaultMongoTemplate.builder().mongoDatabase(mongoClient, DATABASE).build();
        mongoTemplate.deadLetterCollection().deleteMany(Filters.empty());
        return new MongoSequencedDeadLetterQueue<>(
                PROCESSING_GROUP,
                mongoTemplate.deadLetterCollection(),
                new MongoTransactionalExecutorProvider(mongoClient),
                new DelegatingEventConverter(jacksonConverter),
                jacksonConverter,
                MongoSequencedDeadLetterQueueConfiguration.DEFAULT
                        .maxSequences(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
                        .maxSequenceSize(MAX_SEQUENCES_AND_SEQUENCE_SIZE)
        );
    }

    @Override
    protected UnitOfWorkFactory buildUnitOfWorkFactory() {
        return new TransactionalUnitOfWorkFactory(
                new ClientSessionTransactionManager(mongoClient),
                new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE)
        );
    }

    @AfterEach
    void closeClient() {
        mongoClient.close();
    }
}
