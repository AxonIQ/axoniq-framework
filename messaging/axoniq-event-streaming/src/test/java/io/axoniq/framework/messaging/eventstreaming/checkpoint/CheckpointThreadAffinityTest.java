/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.messaging.eventstreaming.checkpoint;

import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.core.unitofwork.transaction.Transaction;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.eventhandling.AsyncInMemoryStreamableEventSource;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests that the checkpoint strategy saves tokens on the processor's own thread, inside its transaction, while a
 * participant confirms checkpoints from another thread. Covers batch commits, release, split, merge and shutdown.
 */
class CheckpointThreadAffinityTest {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointThreadAffinityTest.class);
    private static final String PROCESSOR_NAME = "affinity";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private AxonConfiguration configuration;
    private final ExecutorService foreign = Executors.newSingleThreadExecutor(r -> new Thread(r, "foreign-wf"));

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
        foreign.shutdownNow();
    }

    @Test
    void batchCommitStoreRunsOnForeignThread() {
        ThreadBoundTransactionManager tm = new ThreadBoundTransactionManager(true);
        CheckingTokenStore store = new CheckingTokenStore();
        AsyncParticipant participant = new AsyncParticipant(foreign, true);
        start(tm, store, 1, participant);

        publish(1, participant);
        participant.trigger.requestCheckpoint();
        eventSource.publishMessage(EventTestUtils.asEventMessage("event-x"));

        await().atMost(TIMEOUT).until(() -> !store.violations.isEmpty() || store.stores.get() > 0);
        report("batch", tm, store, participant);
        assertThat(store.violations).as("store without the batch transaction on the calling thread").isEmpty();
    }

    @Test
    void idleTriggerCycleStoreRunsOnForeignThread() {
        ThreadBoundTransactionManager tm = new ThreadBoundTransactionManager(true);
        CheckingTokenStore store = new CheckingTokenStore();
        AsyncParticipant participant = new AsyncParticipant(foreign, true);
        start(tm, store, 1, participant);

        publish(3, participant);
        // confirm-then-store style: the request arrives between cycles, from the foreign thread
        foreign.execute(() -> participant.trigger.requestCheckpoint());

        await().atMost(TIMEOUT).until(() -> !store.violations.isEmpty() || store.stores.get() > 0);
        report("idle-trigger", tm, store, participant);
        assertThat(store.violations).isEmpty();
    }

    @Test
    void autoModeEveryBatchStoresOnForeignThread() {
        ThreadBoundTransactionManager tm = new ThreadBoundTransactionManager(true);
        CheckingTokenStore store = new CheckingTokenStore();
        AsyncParticipant participant = new AsyncParticipant(foreign, true);
        start(tm, store, 1, participant, new OrdinaryHandler());

        eventSource.publishMessage(EventTestUtils.asEventMessage("event-x"));

        await().atMost(TIMEOUT).until(() -> !store.violations.isEmpty() || store.stores.get() > 0);
        report("auto", tm, store, participant);
        assertThat(store.violations).isEmpty();
    }

    @Test
    void releaseSegmentStoresAndReleasesClaimOnForeignThread() {
        ThreadBoundTransactionManager tm = new ThreadBoundTransactionManager(true);
        CheckingTokenStore store = new CheckingTokenStore();
        AsyncParticipant participant = new AsyncParticipant(foreign, false);
        start(tm, store, 1, participant);
        publish(3, participant);
        participant.async = true;

        processor().releaseSegment(0);

        await().atMost(TIMEOUT).until(() -> participant.released.get() > 0);
        await().pollDelay(Duration.ofSeconds(1)).atMost(TIMEOUT).until(() -> true);
        report("release", tm, store, participant);
        assertThat(store.violations).isEmpty();
    }

    @Test
    void splitStoresOnForeignThread() {
        ThreadBoundTransactionManager tm = new ThreadBoundTransactionManager(true);
        CheckingTokenStore store = new CheckingTokenStore();
        AsyncParticipant participant = new AsyncParticipant(foreign, false);
        start(tm, store, 1, participant);
        publish(3, participant);
        participant.async = true;

        Boolean split = processor().splitSegment(0).orTimeout(10, TimeUnit.SECONDS).join();

        report("split", tm, store, participant);
        logger.warn("split result={} storedPos0={} storedPos1={}", split, storedPosition(store, 0),
                    storedPosition(store, 1));
        assertThat(store.violations).isEmpty();
    }

    @Test
    void mergeStoresOnForeignThread() {
        ThreadBoundTransactionManager tm = new ThreadBoundTransactionManager(true);
        CheckingTokenStore store = new CheckingTokenStore();
        AsyncParticipant participant = new AsyncParticipant(foreign, false);
        start(tm, store, 2, participant);
        publish(4, participant);
        participant.async = true;

        Boolean merged = processor().mergeSegment(0).orTimeout(10, TimeUnit.SECONDS).join();

        report("merge", tm, store, participant);
        logger.warn("merge result={}", merged);
        assertThat(store.violations).isEmpty();
    }

    @Test
    void shutdownWithNeverCompletingReleaseFutureReturns() {
        ThreadBoundTransactionManager tm = new ThreadBoundTransactionManager(true);
        CheckingTokenStore store = new CheckingTokenStore();
        AsyncParticipant participant = new AsyncParticipant(foreign, false);
        start(tm, store, 1, participant);
        publish(2, participant);
        participant.neverComplete = true;

        long begin = System.nanoTime();
        CompletableFuture<Void> shutdown = CompletableFuture.runAsync(() -> configuration.shutdown());
        boolean returned;
        try {
            shutdown.get(30, TimeUnit.SECONDS);
            returned = true;
        } catch (Exception e) {
            returned = false;
        }
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
        logger.warn("shutdown-never-complete returned={} afterMs={} releasedCalls={} claimReleases={}",
                    returned, millis, participant.released.get(), store.releases.get());
        configuration = null;
        assertThat(returned).isTrue();
    }

    @Test
    void asyncTransactionManagerCommitsOnForeignThread() {
        ThreadBoundTransactionManager tm = new ThreadBoundTransactionManager(false);
        CheckingTokenStore store = new CheckingTokenStore();
        AsyncParticipant participant = new AsyncParticipant(foreign, true);
        start(tm, store, 1, participant);

        publish(1, participant);
        participant.trigger.requestCheckpoint();
        eventSource.publishMessage(EventTestUtils.asEventMessage("event-x"));

        await().atMost(TIMEOUT).until(() -> !store.violations.isEmpty() || store.stores.get() > 0);
        await().pollDelay(Duration.ofMillis(500)).atMost(TIMEOUT).until(() -> true);
        report("async-tm", tm, store, participant);
        assertThat(tm.offThreadCommits).isEmpty();
    }

    @Test
    void resetTokensWithTwoParticipantsKeepsCheckpointing() {
        ThreadBoundTransactionManager tm = new ThreadBoundTransactionManager(true);
        CheckingTokenStore store = new CheckingTokenStore();
        AsyncParticipant first = new AsyncParticipant(foreign, false);
        AsyncParticipant second = new AsyncParticipant(foreign, false);
        first.requestOnEach = true;
        start(tm, store, 1, first, second);
        publish(5, first);
        await().pollDelay(Duration.ofSeconds(2)).atMost(TIMEOUT).until(() -> true);
        long before = storedPosition(store, 0);
        logger.warn("reset-pre stored={} handled={} advances={}",
                    FutureUtils.joinAndUnwrap(store.fetchToken(PROCESSOR_NAME, 0, null)), first.handled.get(),
                    first.advanceThreads.size());

        processor().shutdown().join();
        processor().resetTokens().join();
        processor().start().join();
        int handledBefore = first.handled.get();
        await().atMost(TIMEOUT).until(() -> first.handled.get() >= handledBefore + 5);
        eventSource.publishMessage(EventTestUtils.asEventMessage("after-reset"));
        await().pollDelay(Duration.ofSeconds(3)).atMost(TIMEOUT).until(() -> true);
        TrackingToken stored = FutureUtils.joinAndUnwrap(store.fetchToken(PROCESSOR_NAME, 0, null));
        logger.warn("reset storedBefore={} storedAfter={} handledAfterReset={} violations={}", before, stored,
                    first.handled.get() - handledBefore, store.violations);
        assertThat(first.handled.get() - handledBefore).isLessThan(20);
        assertThat(storedPosition(store, 0)).isGreaterThanOrEqualTo(5);
    }

    // ---------------------------------------------------------------------------------------------------------------

    private void report(String scenario, ThreadBoundTransactionManager tm, CheckingTokenStore store,
                        AsyncParticipant participant) {
        logger.warn("{} violations={} offThreadCommits={} stores={} advanceThreads={} maxConcurrentAdvances={}",
                    scenario, store.violations, tm.offThreadCommits, store.stores.get(), participant.advanceThreads,
                    participant.maxConcurrent.get());
    }

    private void publish(int count, AsyncParticipant participant) {
        int before = participant.handled.get();
        for (int i = 0; i < count; i++) {
            eventSource.publishMessage(EventTestUtils.asEventMessage("event-" + i));
        }
        await().atMost(TIMEOUT).until(() -> participant.handled.get() >= before + count);
    }

    private AsyncInMemoryStreamableEventSource eventSource;

    private void start(TransactionManager tm, TokenStore store, int segments, Object... components) {
        eventSource = new AsyncInMemoryStreamableEventSource(false, false);
        UnitOfWorkFactory uowFactory = new TransactionalUnitOfWorkFactory(tm, UnitOfWorkTestUtils.SIMPLE_FACTORY);
        var module = EventProcessorModule.pooledStreaming(PROCESSOR_NAME)
                                         .eventHandlingComponents(c -> {
                                             var phase = c.autodetected("c0", cfg -> components[0]);
                                             for (int i = 1; i < components.length; i++) {
                                                 Object component = components[i];
                                                 phase = phase.autodetected("c" + i, cfg -> component);
                                             }
                                             return phase;
                                         })
                                         .customized((cfg, c) -> c.eventSource(eventSource)
                                                                  .tokenStore(store)
                                                                  .unitOfWorkFactory(uowFactory)
                                                                  .initialSegmentCount(segments));
        configuration = MessagingConfigurer.create()
                                           .eventProcessing(ep -> ep.pooledStreaming(ps -> ps.processor(module)))
                                           .build();
        configuration.start();
    }

    private PooledStreamingEventProcessor processor() {
        return (PooledStreamingEventProcessor) configuration.getComponents(EventProcessor.class).get(PROCESSOR_NAME);
    }

    private static long storedPosition(TokenStore tokenStore, int segmentId) {
        try {
            TrackingToken token = FutureUtils.joinAndUnwrap(tokenStore.fetchToken(PROCESSOR_NAME, segmentId, null));
            return token == null ? -1L : token.position().orElse(-1L);
        } catch (Exception e) {
            return -2L;
        }
    }

    /**
     * Binds a transaction to the thread that began it, like Spring's transaction synchronization.
     */
    static final class ThreadBoundTransactionManager implements TransactionManager {

        static final ThreadLocal<Object> BOUND = new ThreadLocal<>();
        static final Context.ResourceKey<Object> TX_KEY = Context.ResourceKey.withLabel("threadBoundTx");
        private final boolean sameThread;
        final List<String> offThreadCommits = new CopyOnWriteArrayList<>();

        ThreadBoundTransactionManager(boolean sameThread) {
            this.sameThread = sameThread;
        }

        @Override
        public Transaction startTransaction() {
            if (BOUND.get() != null) {
                return new Transaction() {
                    @Override
                    public void commit() {
                    }

                    @Override
                    public void rollback() {
                    }
                };
            }
            Object tx = new Object();
            Thread starter = Thread.currentThread();
            BOUND.set(tx);
            return new Transaction() {
                @Override
                public void commit() {
                    conclude("commit");
                }

                @Override
                public void rollback() {
                    conclude("rollback");
                }

                private void conclude(String what) {
                    if (Thread.currentThread() != starter) {
                        offThreadCommits.add(what + " on " + Thread.currentThread().getName() + " began on "
                                                     + starter.getName());
                    } else {
                        BOUND.remove();
                    }
                }
            };
        }

        @Override
        public void attachToProcessingLifecycle(ProcessingLifecycle processingLifecycle) {
            processingLifecycle.runOnPreInvocation(pc -> {
                Transaction transaction = startTransaction();
                pc.putResource(TX_KEY, BOUND.get());
                pc.runOnCommit(p -> transaction.commit());
                pc.onError((p, phase, e) -> transaction.rollback());
            });
        }

        @Override
        public boolean requiresSameThreadInvocations() {
            return sameThread;
        }
    }

    /**
     * Refuses a write when the calling thread holds no transaction, or a different one than the context's.
     */
    static final class CheckingTokenStore implements TokenStore {

        private final InMemoryTokenStore delegate = new InMemoryTokenStore();
        final List<String> violations = new CopyOnWriteArrayList<>();
        final AtomicInteger stores = new AtomicInteger();
        final AtomicInteger releases = new AtomicInteger();

        private <T> @Nullable CompletableFuture<T> check(String op, @Nullable ProcessingContext ctx) {
            if (ctx == null) {
                return null;
            }
            Object expected = ctx.getResource(ThreadBoundTransactionManager.TX_KEY);
            Object bound = ThreadBoundTransactionManager.BOUND.get();
            if (expected == null || bound != expected) {
                String v = op + " on " + Thread.currentThread().getName()
                        + (bound == null ? " (no tx bound)" : " (other tx bound)");
                violations.add(v);
                return CompletableFuture.failedFuture(new IllegalStateException("No transaction for " + v));
            }
            return null;
        }

        @Override
        public CompletableFuture<List<Segment>> initializeTokenSegments(String processorName, int segmentCount,
                                                                        @Nullable TrackingToken initialToken,
                                                                        @Nullable ProcessingContext context) {
            CompletableFuture<List<Segment>> f = check("initializeTokenSegments", context);
            return f != null ? f : delegate.initializeTokenSegments(processorName, segmentCount, initialToken, context);
        }

        @Override
        public CompletableFuture<Void> storeToken(@Nullable TrackingToken token, String processorName, int segmentId,
                                                  @Nullable ProcessingContext context) {
            CompletableFuture<Void> f = check("storeToken", context);
            if (f == null) {
                stores.incrementAndGet();
            }
            return f != null ? f : delegate.storeToken(token, processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName, int segmentId,
                                                           @Nullable ProcessingContext context) {
            CompletableFuture<TrackingToken> f = check("fetchToken", context);
            return f != null ? f : delegate.fetchToken(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Void> extendClaim(String processorName, int segmentId,
                                                   @Nullable ProcessingContext context) {
            CompletableFuture<Void> f = check("extendClaim", context);
            return f != null ? f : delegate.extendClaim(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Void> releaseClaim(String processorName, int segmentId,
                                                    @Nullable ProcessingContext context) {
            CompletableFuture<Void> f = check("releaseClaim", context);
            if (f == null) {
                releases.incrementAndGet();
            }
            return f != null ? f : delegate.releaseClaim(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Void> initializeSegment(@Nullable TrackingToken token, String processorName,
                                                         Segment segment, @Nullable ProcessingContext context) {
            CompletableFuture<Void> f = check("initializeSegment", context);
            return f != null ? f : delegate.initializeSegment(token, processorName, segment, context);
        }

        @Override
        public CompletableFuture<Void> deleteToken(String processorName, int segmentId,
                                                   @Nullable ProcessingContext context) {
            CompletableFuture<Void> f = check("deleteToken", context);
            return f != null ? f : delegate.deleteToken(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Segment> fetchSegment(String processorName, int segmentId,
                                                       @Nullable ProcessingContext context) {
            CompletableFuture<Segment> f = check("fetchSegment", context);
            return f != null ? f : delegate.fetchSegment(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<List<Segment>> fetchSegments(String processorName,
                                                              @Nullable ProcessingContext context) {
            CompletableFuture<List<Segment>> f = check("fetchSegments", context);
            return f != null ? f : delegate.fetchSegments(processorName, context);
        }

        @Override
        public CompletableFuture<List<Segment>> fetchAvailableSegments(String processorName,
                                                                       @Nullable ProcessingContext context) {
            CompletableFuture<List<Segment>> f = check("fetchAvailableSegments", context);
            return f != null ? f : delegate.fetchAvailableSegments(processorName, context);
        }

        @Override
        public CompletableFuture<String> retrieveStorageIdentifier(@Nullable ProcessingContext context) {
            return delegate.retrieveStorageIdentifier(context);
        }
    }

    /**
     * Completes checkpoint futures on a foreign thread when {@link #async} is set, like the workflow engine's latch.
     */
    static final class AsyncParticipant implements Checkpointing {

        private final ExecutorService foreign;
        volatile boolean async;
        volatile boolean neverComplete;
        volatile boolean requestOnEach;
        volatile CheckpointTrigger trigger;
        final AtomicInteger handled = new AtomicInteger();
        final AtomicInteger released = new AtomicInteger();
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxConcurrent = new AtomicInteger();
        final List<String> advanceThreads = new CopyOnWriteArrayList<>();

        AsyncParticipant(ExecutorService foreign, boolean async) {
            this.foreign = foreign;
            this.async = async;
        }

        @EventHandler
        void on(String event, CheckpointTrigger checkpoint) {
            this.trigger = checkpoint;
            handled.incrementAndGet();
            if (requestOnEach) {
                checkpoint.requestCheckpoint();
            }
        }

        @Override
        public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment, TrackingToken requested) {
            advanceThreads.add(Thread.currentThread().getName());
            int now = inFlight.incrementAndGet();
            maxConcurrent.accumulateAndGet(now, Math::max);
            if (neverComplete) {
                return new CompletableFuture<>();
            }
            if (!async) {
                inFlight.decrementAndGet();
                return CompletableFuture.completedFuture(requested);
            }
            CompletableFuture<TrackingToken> result = new CompletableFuture<>();
            foreign.execute(() -> {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                inFlight.decrementAndGet();
                result.complete(requested);
            });
            return result;
        }

        @Override
        public CompletableFuture<TrackingToken> onSegmentReleased(Segment segment, TrackingToken upTo) {
            released.incrementAndGet();
            return onCheckpointAdvanced(segment, upTo);
        }
    }

    /**
     * An ordinary handler, so the processor runs in auto mode.
     */
    static final class OrdinaryHandler {

        @EventHandler
        void on(String event) {
        }
    }
}
