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
import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.GenericDeadLetter;
import io.axoniq.framework.mongo.DefaultMongoTemplate;
import io.axoniq.framework.mongo.MongoTemplate;
import io.axoniq.framework.mongo.transaction.ClientSessionTransactionManager;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import io.axoniq.framework.mongo.util.MongoTestImage;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates that the operations of the {@link MongoSequencedDeadLetterQueue} take part in the MongoDB transaction of
 * the processing context they are invoked with.
 *
 * @author Steven van Beelen
 */
@Testcontainers
class MongoSequencedDeadLetterQueueTransactionIT {

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer(MongoTestImage.NAME).withReplicaSet();

    private static final String DATABASE = "dead-letter-transaction-test";
    private static final String SEQUENCE = "sequence";

    private MongoClient mongoClient;
    private MongoSequencedDeadLetterQueue<EventMessage> queue;
    private UnitOfWorkFactory unitOfWorkFactory;

    @BeforeEach
    void setUp() {
        mongoClient = MongoClients.create(MONGO_CONTAINER.getReplicaSetUrl(DATABASE));
        MongoTemplate mongoTemplate = DefaultMongoTemplate.builder().mongoDatabase(mongoClient, DATABASE).build();
        mongoTemplate.deadLetterCollection().deleteMany(Filters.empty());
        JacksonConverter jacksonConverter = new JacksonConverter();
        queue = new MongoSequencedDeadLetterQueue<>("processing-group",
                                                    mongoTemplate.deadLetterCollection(),
                                                    new MongoTransactionalExecutorProvider(mongoClient),
                                                    new DelegatingEventConverter(jacksonConverter),
                                                    jacksonConverter,
                                                    MongoSequencedDeadLetterQueueConfiguration.DEFAULT);
        unitOfWorkFactory = new TransactionalUnitOfWorkFactory(
                new ClientSessionTransactionManager(mongoClient),
                new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE)
        );
    }

    @AfterEach
    void tearDown() {
        mongoClient.close();
    }

    private static DeadLetter<EventMessage> letter() {
        return new GenericDeadLetter<>(SEQUENCE, EventTestUtils.asEventMessage("payload"));
    }

    private long committedSize() {
        return queue.size(null).orTimeout(30, TimeUnit.SECONDS).join();
    }

    @Nested
    class WhenTheProcessingLifecycleCommits {

        @Test
        void enqueuedLetterIsVisibleOnlyAfterTheCommit() throws Exception {
            // given
            AtomicLong visibleBeforeCommit = new AtomicLong(-1);

            // when
            unitOfWorkFactory.create().executeWithResult(
                    context -> queue.enqueue(SEQUENCE, letter(), context).thenApply(v -> {
                        visibleBeforeCommit.set(committedSize());
                        return null;
                    })
            ).get(30, TimeUnit.SECONDS);

            // then
            assertThat(visibleBeforeCommit).hasValue(0);
            assertThat(committedSize()).isEqualTo(1);
        }

        @Test
        void sequenceEnqueuedInOneProcessingContextIsCommittedInOneGo() throws Exception {
            // given / when
            unitOfWorkFactory.create().executeWithResult(
                    context -> queue.enqueue(SEQUENCE, letter(), context)
                                    .thenCompose(v -> queue.enqueue(SEQUENCE, letter(), context))
            ).get(30, TimeUnit.SECONDS);

            // then
            assertThat(queue.sequenceSize(SEQUENCE, null).orTimeout(30, TimeUnit.SECONDS).join()).isEqualTo(2);
        }

        @Test
        void evictedLetterIsRemovedOnlyAfterTheCommit() throws Exception {
            // given
            queue.enqueue(SEQUENCE, letter(), null).orTimeout(30, TimeUnit.SECONDS).join();
            AtomicLong visibleBeforeCommit = new AtomicLong(-1);

            // when
            unitOfWorkFactory.create().executeWithResult(
                    context -> firstLetter(context)
                            .thenCompose(letter -> queue.evict(letter, context))
                            .thenApply(v -> {
                                visibleBeforeCommit.set(committedSize());
                                return null;
                            })
            ).get(30, TimeUnit.SECONDS);

            // then
            assertThat(visibleBeforeCommit).hasValue(1);
            assertThat(committedSize()).isZero();
        }
    }

    @Nested
    class WhenTheProcessingLifecycleFails {

        @Test
        void enqueuedLetterIsRolledBack() {
            // given / when
            CompletableFuture<Object> result = unitOfWorkFactory.create().executeWithResult(
                    context -> queue.enqueue(SEQUENCE, letter(), context).thenCompose(
                            v -> CompletableFuture.failedFuture(new IllegalStateException("Handling failed"))
                    )
            );

            // then
            assertThatThrownBy(() -> result.get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(committedSize()).isZero();
        }

        @Test
        void evictedLetterRemainsInTheQueue() {
            // given
            queue.enqueue(SEQUENCE, letter(), null).orTimeout(30, TimeUnit.SECONDS).join();

            // when
            CompletableFuture<Object> result = unitOfWorkFactory.create().executeWithResult(
                    context -> firstLetter(context)
                            .thenCompose(letter -> queue.evict(letter, context))
                            .thenCompose(v -> CompletableFuture.failedFuture(
                                    new IllegalStateException("Handling failed")
                            ))
            );

            // then
            assertThatThrownBy(() -> result.get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(committedSize()).isEqualTo(1);
        }

        @Test
        void requeuedLetterIsRolledBack() {
            // given
            queue.enqueue(SEQUENCE, letter(), null).orTimeout(30, TimeUnit.SECONDS).join();
            DeadLetter<? extends EventMessage> original = firstLetter(null).orTimeout(30, TimeUnit.SECONDS).join();

            // when
            CompletableFuture<Object> result = unitOfWorkFactory.create().executeWithResult(
                    context -> queue.requeue(original,
                                             l -> l.withCause(new IllegalArgumentException("requeued")),
                                             context)
                                    .thenCompose(v -> CompletableFuture.failedFuture(
                                            new IllegalStateException("Handling failed")
                                    ))
            );

            // then
            assertThatThrownBy(() -> result.get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(firstLetter(null).orTimeout(30, TimeUnit.SECONDS).join().cause()).isEmpty();
        }
    }

    @Nested
    class WhenNoSessionIsBoundToTheProcessingContext {

        @Test
        void operationFailsInTheSameWayAsTheJpaAndJdbcQueues() {
            // given
            StubProcessingContext contextWithoutSession = new StubProcessingContext();

            // when
            CompletableFuture<Void> result = queue.enqueue(SEQUENCE, letter(), contextWithoutSession);

            // then
            assertThatThrownBy(() -> result.get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(committedSize()).isZero();
        }
    }

    private CompletableFuture<DeadLetter<? extends EventMessage>> firstLetter(
            org.axonframework.messaging.core.unitofwork.ProcessingContext context
    ) {
        return queue.deadLetterSequence(SEQUENCE, context).thenApply(sequence -> sequence.iterator().next());
    }
}
