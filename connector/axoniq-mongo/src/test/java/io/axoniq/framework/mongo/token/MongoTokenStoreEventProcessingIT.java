/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.token;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.axoniq.framework.mongo.DefaultMongoTemplate;
import io.axoniq.framework.mongo.MongoTemplate;
import io.axoniq.framework.mongo.transaction.ClientSessionTransactionManager;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import io.axoniq.framework.mongo.util.MongoTestImage;
import org.axonframework.common.FutureUtils;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.AsyncInMemoryStreamableEventSource;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.axonframework.messaging.eventhandling.EventTestUtils.asEventMessage;

/**
 * Integration test validating that a {@link PooledStreamingEventProcessor} using the {@link MongoTokenStore} stores its
 * tracking token in the same MongoDB transaction as the projection its event handler updates.
 * <p>
 * Requires a MongoDB replica set, as multi-document transactions are not supported otherwise.
 *
 * @author Steven van Beelen
 */
@Testcontainers
class MongoTokenStoreEventProcessingIT {

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer(MongoTestImage.NAME).withReplicaSet();

    private static final String DATABASE = "event-processing-it";
    private static final String PROCESSOR = "projecting-processor";
    private static final Duration AWAIT = Duration.ofSeconds(30);

    private final JacksonConverter converter = new JacksonConverter();
    private final Set<String> failingHandlerFor = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean failTokenUpdate = new AtomicBoolean(false);
    private final AtomicInteger failedHandlerInvocations = new AtomicInteger();
    private final AtomicInteger failedTokenUpdates = new AtomicInteger();
    private final List<Boolean> projectionVisibleOutsideTransaction = new CopyOnWriteArrayList<>();

    private MongoClient mongoClient;
    private MongoCollection<Document> projection;
    private MongoCollection<Document> tokens;
    private AsyncInMemoryStreamableEventSource eventSource;
    private ScheduledExecutorService coordinatorExecutor;
    private ScheduledExecutorService workerExecutor;
    private PooledStreamingEventProcessor processor;

    @BeforeEach
    void setUp() {
        mongoClient = MongoClients.create(MONGO_CONTAINER.getReplicaSetUrl(DATABASE));
        MongoTemplate mongoTemplate = DefaultMongoTemplate.builder().mongoDatabase(mongoClient, DATABASE).build();
        tokens = mongoTemplate.trackingTokensCollection();
        tokens.drop();
        projection = mongoClient.getDatabase(DATABASE).getCollection("projection");
        projection.drop();
        mongoClient.getDatabase(DATABASE).createCollection("projection");

        MongoTransactionalExecutorProvider provider = new MongoTransactionalExecutorProvider(mongoClient);
        TokenStoreWithFailingUpdates tokenStore = new TokenStoreWithFailingUpdates(mongoTemplate, provider);

        SimpleEventHandlingComponent component = SimpleEventHandlingComponent.create(
                "projector", (event, context) -> java.util.Optional.of(event.payloadAs(Projected.class).id())
        );
        component.subscribe(new QualifiedName(Projected.class), projectingHandler(provider));

        // Keeps its events when the processor reopens the stream after a failure, so they can be retried.
        eventSource = new AsyncInMemoryStreamableEventSource(false, false);
        coordinatorExecutor = Executors.newSingleThreadScheduledExecutor();
        workerExecutor = Executors.newSingleThreadScheduledExecutor();
        PooledStreamingEventProcessorConfiguration configuration = new PooledStreamingEventProcessorConfiguration(
                new EventProcessorConfiguration(PROCESSOR, null)
        ).eventSource(eventSource)
         .unitOfWorkFactory(new TransactionalUnitOfWorkFactory(
                 new ClientSessionTransactionManager(mongoClient),
                 new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE)
         ))
         .tokenStore(tokenStore)
         .coordinatorExecutor(coordinatorExecutor)
         .workerExecutor(workerExecutor)
         .initialSegmentCount(1)
         .claimExtensionThreshold(1000);
        processor = new PooledStreamingEventProcessor(PROCESSOR, List.of(component), configuration);
        FutureUtils.joinAndUnwrap(processor.start());
    }

    @AfterEach
    void tearDown() throws Exception {
        processor.shutdown().get(15, TimeUnit.SECONDS);
        coordinatorExecutor.shutdownNow();
        workerExecutor.shutdownNow();
        mongoClient.close();
    }

    /**
     * Updates the projection through the session of the processing context, so it takes part in the transaction that
     * the {@link ClientSessionTransactionManager} controls. Fails after that write for the identifiers in
     * {@link #failingHandlerFor}.
     */
    private EventHandler projectingHandler(MongoTransactionalExecutorProvider provider) {
        return (event, context) -> {
            String id = event.payloadAs(Projected.class).id();
            FutureUtils.joinAndUnwrap(
                    provider.getTransactionalExecutor(context)
                            .accept(session -> projection.insertOne(session, new Document("_id", id)))
            );
            // The write is part of a transaction, so it can not be visible to anyone else before the commit.
            projectionVisibleOutsideTransaction.add(projection.countDocuments(eq("_id", id)) > 0);
            if (failingHandlerFor.contains(id)) {
                failedHandlerInvocations.incrementAndGet();
                throw new IllegalStateException("Handling of [" + id + "] fails after updating the projection");
            }
            return MessageStream.<Message>empty();
        };
    }

    private void publish(String... ids) {
        for (String id : ids) {
            eventSource.publishMessage(asEventMessage(new Projected(id)));
        }
    }

    private long projected(String id) {
        return projection.countDocuments(eq("_id", id));
    }

    private long storedPosition() {
        Document document = tokens.find(eq("processorName", PROCESSOR)).first();
        if (document == null || document.get("token") == null) {
            return -1;
        }
        byte[] data = document.get("token", org.bson.types.Binary.class).getData();
        return converter.convert(data, GlobalSequenceTrackingToken.class).getGlobalIndex();
    }

    @Nested
    class HappyPath {

        @Test
        void tokenAndProjectionAreCommittedTogetherAndInvisibleBeforeThat() {
            // given
            publish("first");
            await().atMost(AWAIT).until(() -> projected("first") == 1);
            long positionAfterFirst = storedPosition();

            // when
            publish("second", "third");

            // then
            await().atMost(AWAIT).until(() -> projected("third") == 1);
            assertThat(projected("second")).isEqualTo(1);
            await().atMost(AWAIT).untilAsserted(() -> assertThat(storedPosition()).isEqualTo(positionAfterFirst + 2));
            assertThat(projectionVisibleOutsideTransaction).hasSize(3).containsOnly(false);
        }
    }

    @Nested
    class FaultyPath {

        @Test
        void handlerFailureAfterTheProjectionWriteRollsBackTheProjectionAndTheToken() {
            // given
            publish("ok");
            await().atMost(AWAIT).until(() -> projected("ok") == 1);
            long positionBeforeFailure = storedPosition();
            failingHandlerFor.add("poison");

            // when
            publish("poison");
            await().atMost(AWAIT).until(() -> failedHandlerInvocations.get() > 0);

            // then
            assertThat(projected("poison")).isZero();
            assertThat(storedPosition()).isEqualTo(positionBeforeFailure);

            // when the failure resolves, the rolled back write is applied exactly once
            failingHandlerFor.clear();

            // then
            await().atMost(AWAIT).until(() -> projected("poison") == 1);
            await().atMost(AWAIT).untilAsserted(
                    () -> assertThat(storedPosition()).isEqualTo(positionBeforeFailure + 1)
            );
        }

        @Test
        void failureAfterTheTokenUpdateRollsBackTheTokenAndTheProjection() {
            // given
            publish("ok");
            await().atMost(AWAIT).until(() -> projected("ok") == 1);
            long positionBeforeFailure = storedPosition();
            failTokenUpdate.set(true);

            // when
            publish("unlucky");
            await().atMost(AWAIT).until(() -> failedTokenUpdates.get() > 0);

            // then
            assertThat(projected("unlucky")).isZero();
            assertThat(storedPosition()).isEqualTo(positionBeforeFailure);

            // when the failure resolves, the rolled back write is applied exactly once
            failTokenUpdate.set(false);

            // then
            await().atMost(AWAIT).until(() -> projected("unlucky") == 1);
            await().atMost(AWAIT).untilAsserted(
                    () -> assertThat(storedPosition()).isEqualTo(positionBeforeFailure + 1)
            );
        }
    }

    /**
     * A {@link MongoTokenStore} that fails on demand once the token has been stored, to simulate a failure after both
     * the event handler updated its projection and the token was written.
     */
    private class TokenStoreWithFailingUpdates extends MongoTokenStore {

        TokenStoreWithFailingUpdates(MongoTemplate mongoTemplate, MongoTransactionalExecutorProvider provider) {
            super(mongoTemplate.trackingTokensCollection(), provider, converter, MongoTokenStoreConfiguration.DEFAULT);
        }

        @Override
        public CompletableFuture<Void> storeToken(@Nullable TrackingToken token,
                                                  String processorName,
                                                  int segmentId,
                                                  ProcessingContext context) {
            return super.storeToken(token, processorName, segmentId, context).thenCompose(stored -> {
                if (failTokenUpdate.get()) {
                    failedTokenUpdates.incrementAndGet();
                    return CompletableFuture.failedFuture(new IllegalStateException("Failing after storing the token"));
                }
                return CompletableFuture.completedFuture(stored);
            });
        }
    }

    private record Projected(String id) {

    }
}
