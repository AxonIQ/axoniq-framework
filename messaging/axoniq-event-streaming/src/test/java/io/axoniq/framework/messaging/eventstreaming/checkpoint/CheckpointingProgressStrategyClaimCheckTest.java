/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.eventstreaming.checkpoint;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.jpa.EntityManagerExecutor;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.core.unitofwork.transaction.Transaction;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.AsyncInMemoryStreamableEventSource;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.TokenEntry;
import org.hibernate.jpa.HibernatePersistenceConfiguration;
import org.hibernate.tool.schema.Action;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests that the {@link CheckpointingProgressStrategy} lets the writes a participant makes within the transaction of a
 * checkpoint commit only while this node owns the claim of the segment, also when the checkpoint does not advance the
 * stored token, so the token store has nothing to store and does not check the owner of the claim itself.
 * <p>
 * Runs a pooled streaming processor whose units of work run in JPA transactions, with a {@link JpaTokenStore} in the
 * same database as the writes of its participant.
 *
 * @author Allard Buijze
 */
class CheckpointingProgressStrategyClaimCheckTest {

    private static final String PROCESSOR_NAME = "claim-check";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final EntityManagerFactory entityManagerFactory =
            new HibernatePersistenceConfiguration("checkpoint-claim-check")
                    .managedClasses(List.of(TokenEntry.class))
                    .jdbcUrl("jdbc:hsqldb:mem:" + UUID.randomUUID())
                    .jdbcCredentials("sa", "")
                    .schemaToolingAction(Action.CREATE_DROP)
                    .createEntityManagerFactory();
    private final AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource(false, false);
    private final ClaimCountingTokenStore tokenStore = new ClaimCountingTokenStore(new JpaTokenStore(
            new JpaTransactionalExecutorProvider(entityManagerFactory),
            new JacksonConverter(),
            JpaTokenStoreConfiguration.DEFAULT.nodeId("this-node")
    ));
    private final WritingParticipant participant = new WritingParticipant();
    private @Nullable AxonConfiguration configuration;

    @BeforeEach
    void createParticipantTable() {
        inTransaction(em -> em.createNativeQuery("create table participant_write (id integer)").executeUpdate());
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
        entityManagerFactory.close();
    }

    @Test
    void rollsBackTheWritesOfACheckpointAtTheStoredTokenOnceAnotherNodeOwnsTheClaim() {
        // given - a checkpoint stored with the write of the participant
        start();
        eventSource.publishMessage(EventTestUtils.asEventMessage("event-1"));
        await().atMost(TIMEOUT).until(() -> participant.trigger != null);
        Objects.requireNonNull(participant.trigger).requestCheckpoint();
        await().atMost(TIMEOUT).until(() -> participantWrites() == 1);
        // and - another node took over the segment, without this node noticing yet
        stealClaim();

        // when - a checkpoint at the token that is already stored
        int attempts = participant.writes.get();
        Objects.requireNonNull(participant.trigger).requestCheckpoint();
        await().atMost(TIMEOUT).until(() -> participant.writes.get() > attempts);

        // then - the write of the participant is rolled back, and this node lets go of the segment
        await().atMost(TIMEOUT).until(() -> participantWrites() > 1 || processor().processingStatus().isEmpty());
        assertThat(participantWrites()).isEqualTo(1);
    }

    @Test
    void commitsTheWritesOfACheckpointAtTheStoredTokenWhileThisNodeOwnsTheClaim() {
        // given
        start();
        eventSource.publishMessage(EventTestUtils.asEventMessage("event-1"));
        await().atMost(TIMEOUT).until(() -> participant.trigger != null);
        Objects.requireNonNull(participant.trigger).requestCheckpoint();
        await().atMost(TIMEOUT).until(() -> participantWrites() == 1);
        int claimChecks = tokenStore.extensions.get();

        // when - a checkpoint at the token that is already stored
        Objects.requireNonNull(participant.trigger).requestCheckpoint();

        // then - the write commits, after one check of the claim
        await().atMost(TIMEOUT).until(() -> participantWrites() == 2);
        assertThat(tokenStore.extensions).hasValue(claimChecks + 1);
        assertThat(processor().processingStatus()).containsKey(0);
    }

    @Test
    void checksNoClaimForACheckpointThatStoresTheToken() {
        // given
        start();
        eventSource.publishMessage(EventTestUtils.asEventMessage("event-1"));
        await().atMost(TIMEOUT).until(() -> participant.trigger != null);
        int stores = tokenStore.stores.get();

        // when - a checkpoint that advances the stored token
        Objects.requireNonNull(participant.trigger).requestCheckpoint();

        // then - the store checks the owner of the claim, so no claim extension is needed
        await().atMost(TIMEOUT).until(() -> participantWrites() == 1 && tokenStore.stores.get() > stores);
        assertThat(tokenStore.extensions).hasValue(0);
    }

    private void start() {
        var unitOfWorkFactory = new TransactionalUnitOfWorkFactory(new EntityManagerPerTransaction(),
                                                                   UnitOfWorkTestUtils.SIMPLE_FACTORY);
        var module = EventProcessorModule.pooledStreaming(PROCESSOR_NAME)
                                         .eventHandlingComponents(c -> c.autodetected("participant",
                                                                                      cfg -> participant))
                                         .customized((cfg, c) -> c.eventSource(eventSource)
                                                                  .tokenStore(tokenStore)
                                                                  .unitOfWorkFactory(unitOfWorkFactory)
                                                                  .initialSegmentCount(1)
                                                                  // No claim extensions of the processor itself
                                                                  .claimExtensionThreshold(60_000));
        configuration = MessagingConfigurer.create()
                                           .eventProcessing(ep -> ep.pooledStreaming(ps -> ps.processor(module)))
                                           .build();
        configuration.start();
        await().atMost(TIMEOUT).until(() -> processor().processingStatus().containsKey(0));
    }

    private PooledStreamingEventProcessor processor() {
        return (PooledStreamingEventProcessor) Objects.requireNonNull(configuration)
                                                      .getComponents(EventProcessor.class)
                                                      .get(PROCESSOR_NAME);
    }

    private int participantWrites() {
        return inTransaction(em -> ((Number) em.createNativeQuery("select count(*) from participant_write")
                                               .getSingleResult()).intValue());
    }

    /**
     * Makes another node the owner of the claim, as a node that claimed the segment after this one stalled.
     */
    private void stealClaim() {
        inTransaction(em -> em.createQuery("update TokenEntry t set t.owner = :owner, t.timestamp = :timestamp "
                                                   + "where t.processorName = :processor")
                              .setParameter("owner", "another-node")
                              .setParameter("timestamp", TokenEntry.computeTokenTimestamp())
                              .setParameter("processor", PROCESSOR_NAME)
                              .executeUpdate());
    }

    private <R> R inTransaction(Function<EntityManager, R> work) {
        try (var em = entityManagerFactory.createEntityManager()) {
            em.getTransaction().begin();
            R result = work.apply(em);
            em.getTransaction().commit();
            return result;
        }
    }

    /**
     * A self-checkpointing component that writes a row within the transaction of every checkpoint it is asked to
     * cover with a context.
     */
    static final class WritingParticipant implements Checkpointing {

        private final AtomicInteger writes = new AtomicInteger();
        private volatile @Nullable CheckpointTrigger trigger;

        @EventHandler
        void on(String event, CheckpointTrigger checkpointTrigger) {
            this.trigger = checkpointTrigger;
        }

        @Override
        public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment, TrackingToken requested) {
            return CompletableFuture.completedFuture(requested);
        }

        @Override
        public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment,
                                                                     TrackingToken requested,
                                                                     ProcessingContext context) {
            var executor = Objects.requireNonNull(context.getResource(JpaTransactionalExecutorProvider.SUPPLIER_KEY))
                                  .get();
            return executor.accept(em -> {
                               em.createNativeQuery("insert into participant_write (id) values (1)")
                                 .executeUpdate();
                               writes.incrementAndGet();
                           })
                           .thenApply(unused -> requested);
        }

        @Override
        public CompletableFuture<TrackingToken> onSegmentReleased(Segment segment, TrackingToken upTo) {
            return CompletableFuture.completedFuture(upTo);
        }
    }

    /**
     * Runs every unit of work in a JPA transaction of its own and binds its {@link EntityManager} to the processing
     * context, as the transaction manager of a Spring application does.
     */
    private final class EntityManagerPerTransaction implements TransactionManager {

        @Override
        public Transaction startTransaction() {
            throw new UnsupportedOperationException("Only runs the units of work of the event processor.");
        }

        @Override
        public void attachToProcessingLifecycle(ProcessingLifecycle processingLifecycle) {
            processingLifecycle.runOnPreInvocation(context -> {
                EntityManager entityManager = entityManagerFactory.createEntityManager();
                entityManager.getTransaction().begin();
                var executor = new EntityManagerExecutor(() -> entityManager);
                context.putResource(JpaTransactionalExecutorProvider.SUPPLIER_KEY, () -> executor);
                AtomicBoolean concluded = new AtomicBoolean();
                context.runOnCommit(committed -> {
                    if (concluded.compareAndSet(false, true)) {
                        try {
                            entityManager.getTransaction().commit();
                        } finally {
                            entityManager.close();
                        }
                    }
                });
                context.onError((failed, phase, error) -> {
                    if (concluded.compareAndSet(false, true)) {
                        try {
                            if (entityManager.getTransaction().isActive()) {
                                entityManager.getTransaction().rollback();
                            }
                        } finally {
                            entityManager.close();
                        }
                    }
                });
            });
        }
    }

    /**
     * Counts the token stores and claim extensions of its delegate.
     */
    static final class ClaimCountingTokenStore implements TokenStore {

        private final TokenStore delegate;
        private final AtomicInteger stores = new AtomicInteger();
        private final AtomicInteger extensions = new AtomicInteger();

        ClaimCountingTokenStore(TokenStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<List<Segment>> initializeTokenSegments(String processorName,
                                                                        int segmentCount,
                                                                        @Nullable TrackingToken initialToken,
                                                                        @Nullable ProcessingContext context) {
            return delegate.initializeTokenSegments(processorName, segmentCount, initialToken, context);
        }

        @Override
        public CompletableFuture<Void> storeToken(@Nullable TrackingToken token,
                                                  String processorName,
                                                  int segmentId,
                                                  @Nullable ProcessingContext context) {
            stores.incrementAndGet();
            return delegate.storeToken(token, processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName,
                                                           int segmentId,
                                                           @Nullable ProcessingContext context) {
            return delegate.fetchToken(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Void> extendClaim(String processorName,
                                                   int segmentId,
                                                   @Nullable ProcessingContext context) {
            extensions.incrementAndGet();
            return delegate.extendClaim(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Void> releaseClaim(String processorName,
                                                    int segmentId,
                                                    @Nullable ProcessingContext context) {
            return delegate.releaseClaim(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Void> initializeSegment(@Nullable TrackingToken token,
                                                         String processorName,
                                                         Segment segment,
                                                         @Nullable ProcessingContext context) {
            return delegate.initializeSegment(token, processorName, segment, context);
        }

        @Override
        public CompletableFuture<Void> deleteToken(String processorName,
                                                   int segmentId,
                                                   @Nullable ProcessingContext context) {
            return delegate.deleteToken(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Segment> fetchSegment(String processorName,
                                                       int segmentId,
                                                       @Nullable ProcessingContext context) {
            return delegate.fetchSegment(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<List<Segment>> fetchSegments(String processorName,
                                                              @Nullable ProcessingContext context) {
            return delegate.fetchSegments(processorName, context);
        }

        @Override
        public CompletableFuture<List<Segment>> fetchAvailableSegments(String processorName,
                                                                       @Nullable ProcessingContext context) {
            return delegate.fetchAvailableSegments(processorName, context);
        }

        @Override
        public CompletableFuture<String> retrieveStorageIdentifier(@Nullable ProcessingContext context) {
            return delegate.retrieveStorageIdentifier(context);
        }
    }
}
