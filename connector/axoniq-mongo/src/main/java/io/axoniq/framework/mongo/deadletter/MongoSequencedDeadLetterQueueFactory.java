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
import io.axoniq.framework.messaging.eventhandling.deadletter.SequencedDeadLetterQueueFactory;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.bson.Document;

import static java.util.Objects.requireNonNull;

/**
 * A {@link SequencedDeadLetterQueueFactory} creating a {@link MongoSequencedDeadLetterQueue} per processing group.
 * <p>
 * All queues created by this factory share the same dead letter {@link MongoCollection} and are scoped by their
 * processing group, so a single MongoDB collection serves every event handling component.
 * <p>
 * Example usage:
 * <pre>{@code
 * SequencedDeadLetterQueueFactory factory =
 *         new MongoSequencedDeadLetterQueueFactory(deadLetterCollection, provider, eventConverter, converter,
 *                                                  MongoSequencedDeadLetterQueueConfiguration.DEFAULT);
 * new DeadLetterQueueConfiguration().enabled().factory(factory);
 * }</pre>
 * When the {@link io.axoniq.framework.mongo.configuration.MongoConfigurationEnhancer} is active, an instance is also
 * available as a component, and Spring Boot applications get one through autoconfiguration.
 *
 * @author Steven van Beelen
 * @see MongoSequencedDeadLetterQueue
 * @since 5.5.0
 */
public class MongoSequencedDeadLetterQueueFactory implements SequencedDeadLetterQueueFactory {

    private final MongoCollection<Document> deadLetterCollection;
    private final TransactionalExecutorProvider<ClientSession> transactionalExecutorProvider;
    private final EventConverter eventConverter;
    private final Converter genericConverter;
    private final MongoSequencedDeadLetterQueueConfiguration queueConfiguration;

    /**
     * Constructs a {@code MongoSequencedDeadLetterQueueFactory}.
     *
     * @param deadLetterCollection the {@link MongoCollection} storing the dead letters
     * @param provider             the {@link TransactionalExecutorProvider} providing the session that every queue
     *                             operation executes with
     * @param eventConverter       the {@link EventConverter} converting the payload and metadata of dead-lettered
     *                             events
     * @param genericConverter     the {@link Converter} converting diagnostics and tracking tokens
     * @param queueConfiguration   the {@link MongoSequencedDeadLetterQueueConfiguration} the created queues are
     *                             configured with
     */
    public MongoSequencedDeadLetterQueueFactory(MongoCollection<Document> deadLetterCollection,
                                                TransactionalExecutorProvider<ClientSession> provider,
                                                EventConverter eventConverter,
                                                Converter genericConverter,
                                                MongoSequencedDeadLetterQueueConfiguration queueConfiguration) {
        this.deadLetterCollection = requireNonNull(deadLetterCollection,
                                                   "The dead letter collection must not be null.");
        this.transactionalExecutorProvider = requireNonNull(provider,
                                                            "The TransactionalExecutorProvider must not be null.");
        this.eventConverter = requireNonNull(eventConverter, "The EventConverter must not be null.");
        this.genericConverter = requireNonNull(genericConverter, "The generic Converter must not be null.");
        this.queueConfiguration = requireNonNull(queueConfiguration,
                                                 "The MongoSequencedDeadLetterQueueConfiguration must not be null.");
    }

    @Override
    public SequencedDeadLetterQueue<EventMessage> create(String processingGroup, Configuration configuration) {
        return new MongoSequencedDeadLetterQueue<>(processingGroup,
                                                   deadLetterCollection,
                                                   transactionalExecutorProvider,
                                                   eventConverter,
                                                   genericConverter,
                                                   queueConfiguration);
    }
}
