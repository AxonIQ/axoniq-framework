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

import org.assertj.core.api.AbstractLongAssert;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.annotation.SequencingPolicy;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.sequencing.FullConcurrencyPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.AsyncInMemoryStreamableEventSource;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.SegmentChangeListener;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests whether other segments keep handling events while a split or merge waits for segment 0. They stop for a slow
 * checkpoint release and, in plain Axon Framework, for a slow batch, but not for a slow release listener.
 */
class SplitMergeFreezeTest {

    private static final Logger logger = LoggerFactory.getLogger(SplitMergeFreezeTest.class);
    private static final String PROCESSOR_NAME = "freeze";
    private static final Duration SLOW = Duration.ofSeconds(3);
    private static final Duration WITHIN_THE_WAIT = Duration.ofSeconds(2);
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource(false, false);
    private AxonConfiguration configuration;

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Nested
    class CheckpointingParticipantSlowToReleaseSegmentZero {

        @Test
        void otherSegmentHandlesNoEventsWhileASplitWaitsForTheRelease() {
            // given
            SlowReleaseParticipant participant = new SlowReleaseParticipant();
            start(2, participant, UnaryOperator.identity());
            publishUntilEverySegmentHandled(participant.handled, 2);
            participant.slowRelease.set(true);

            // when
            CompletableFuture<Boolean> split = processor().splitSegment(0);
            await().atMost(TIMEOUT).untilTrue(participant.releaseStarted);
            long publishedAt = System.nanoTime();
            publishDuring(10);

            // then
            // Flips to isLessThan once the wait for the release no longer blocks the other segment.
            assertThatFirstOtherSegmentHandleMs("split", participant.handled, Set.of(1), split, publishedAt)
                    .isGreaterThanOrEqualTo(WITHIN_THE_WAIT.toMillis());
        }

        @Test
        void otherSegmentsHandleNoEventsWhileAMergeWaitsForTheRelease() {
            // given
            SlowReleaseParticipant participant = new SlowReleaseParticipant();
            start(4, participant, UnaryOperator.identity());
            publishUntilEverySegmentHandled(participant.handled, 4);
            participant.slowRelease.set(true);

            // when
            CompletableFuture<Boolean> merge = processor().mergeSegment(0);
            await().atMost(TIMEOUT).untilTrue(participant.releaseStarted);
            long publishedAt = System.nanoTime();
            publishDuring(10);

            // then
            // Flips to isLessThan once the wait for the release no longer blocks the other segments.
            assertThatFirstOtherSegmentHandleMs("merge", participant.handled, Set.of(1, 3), merge, publishedAt)
                    .isGreaterThanOrEqualTo(WITHIN_THE_WAIT.toMillis());
        }
    }

    @Nested
    class PlainAxonFramework {

        @Test
        void otherSegmentKeepsHandlingEventsWhileASplitWaitsForASlowReleaseListener() {
            // given
            PlainHandler handler = new PlainHandler();
            AtomicBoolean listenerCalled = new AtomicBoolean();
            start(2, handler, c -> c.addSegmentChangeListener(SegmentChangeListener.onRelease(segment -> {
                if (segment.getSegmentId() != 0) {
                    return CompletableFuture.completedFuture(null);
                }
                listenerCalled.set(true);
                return CompletableFuture.runAsync(() -> {
                }, CompletableFuture.delayedExecutor(SLOW.toMillis(), TimeUnit.MILLISECONDS));
            })));
            publishUntilEverySegmentHandled(handler.handled, 2);

            // when
            CompletableFuture<Boolean> split = processor().splitSegment(0);
            await().pollDelay(Duration.ofMillis(300)).atMost(TIMEOUT).until(() -> true);
            long publishedAt = System.nanoTime();
            publishDuring(10);

            // then
            logger.warn("plain-listener releaseListenerCalledOnSplit={}", listenerCalled.get());
            assertThatFirstOtherSegmentHandleMs("plain-listener", handler.handled, Set.of(1), split, publishedAt)
                    .isLessThan(WITHIN_THE_WAIT.toMillis());
        }

        @Test
        void otherSegmentHandlesNoEventsWhileASplitWaitsForASlowBatchOnSegmentZero() {
            // given
            PlainHandler handler = new PlainHandler();
            start(2, handler, UnaryOperator.identity());
            publishUntilEverySegmentHandled(handler.handled, 2);
            handler.slowBatchOnSegmentZero.set(true);
            publishUntil(handler.slowBatchStarted);

            // when
            CompletableFuture<Boolean> split = processor().splitSegment(0);
            await().pollDelay(Duration.ofMillis(300)).atMost(TIMEOUT).until(() -> true);
            long publishedAt = System.nanoTime();
            publishDuring(10);

            // then
            // Flips to isLessThan once the wait for the slow batch no longer blocks the other segment.
            assertThatFirstOtherSegmentHandleMs("plain-slow-batch", handler.handled, Set.of(1), split, publishedAt)
                    .isGreaterThanOrEqualTo(WITHIN_THE_WAIT.toMillis());
        }
    }

    private AbstractLongAssert<?> assertThatFirstOtherSegmentHandleMs(String scenario,
                                                                      Handled handled,
                                                                      Set<Integer> otherSegments,
                                                                      CompletableFuture<Boolean> segmentChange,
                                                                      long publishedAt) {
        boolean handledDuringWait;
        try {
            await().atMost(WITHIN_THE_WAIT).until(() -> handled.duringBy(otherSegments) > 0);
            handledDuringWait = true;
        } catch (Exception e) {
            handledDuringWait = false;
        }
        boolean changeDoneAtCheck = segmentChange.isDone();
        Boolean changeResult = segmentChange.orTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).join();
        long changeDoneMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - publishedAt);
        await().atMost(TIMEOUT).until(() -> handled.duringBy(otherSegments) > 0);
        long firstHandledMs = TimeUnit.NANOSECONDS.toMillis(handled.firstDuringAt(otherSegments) - publishedAt);
        logger.warn("{} handledDuringWait={} changeDoneAtCheck={} changeResult={} changeDoneAfterMs={} "
                            + "firstOtherSegmentHandleAfterMs={}",
                    scenario, handledDuringWait, changeDoneAtCheck, changeResult, changeDoneMs, firstHandledMs);
        assertThat(changeResult).isTrue();
        return assertThat(firstHandledMs)
                .as("ms until segments %s handled an event published while segment 0 was being %s (done after %d ms)",
                    otherSegments, scenario, changeDoneMs);
    }

    private void start(int segments,
                       Object component,
                       UnaryOperator<PooledStreamingEventProcessorConfiguration> customization) {
        var module = EventProcessorModule.pooledStreaming(PROCESSOR_NAME)
                                         .eventHandlingComponents(c -> c.autodetected("c0", cfg -> component))
                                         .customized((cfg, c) -> customization.apply(
                                                 c.eventSource(eventSource)
                                                  .tokenStore(new InMemoryTokenStore())
                                                  .initialSegmentCount(segments)));
        configuration = MessagingConfigurer.create()
                                           .eventProcessing(ep -> ep.pooledStreaming(ps -> ps.processor(module)))
                                           .build();
        configuration.start();
    }

    private PooledStreamingEventProcessor processor() {
        return (PooledStreamingEventProcessor) configuration.getComponents(EventProcessor.class).get(PROCESSOR_NAME);
    }

    private void publishUntilEverySegmentHandled(Handled handled, int segments) {
        await().atMost(TIMEOUT).until(() -> {
            eventSource.publishMessage(EventTestUtils.asEventMessage("warmup"));
            return handled.segmentsSeen() == segments;
        });
    }

    private void publishUntil(AtomicBoolean condition) {
        await().atMost(TIMEOUT).until(() -> {
            eventSource.publishMessage(EventTestUtils.asEventMessage("warmup"));
            return condition.get();
        });
    }

    private void publishDuring(int count) {
        for (int i = 0; i < count; i++) {
            eventSource.publishMessage(EventTestUtils.asEventMessage("during-" + i));
        }
    }

    private static int segmentOf(ProcessingContext context) {
        return Segment.fromContext(context).map(Segment::getSegmentId).orElse(-1);
    }

    static final class Handled {

        private final Map<Integer, Boolean> seen = new ConcurrentHashMap<>();
        private final Map<Integer, Long> firstDuring = new ConcurrentHashMap<>();

        void record(String event, int segment) {
            seen.put(segment, true);
            if (event.startsWith("during-")) {
                firstDuring.putIfAbsent(segment, System.nanoTime());
            }
        }

        int segmentsSeen() {
            return seen.size();
        }

        long duringBy(Set<Integer> segments) {
            return segments.stream().filter(firstDuring::containsKey).count();
        }

        long firstDuringAt(Set<Integer> segments) {
            return segments.stream().map(firstDuring::get).filter(java.util.Objects::nonNull)
                           .min(Long::compare).orElseThrow();
        }
    }

    @SequencingPolicy(type = FullConcurrencyPolicy.class)
    static final class SlowReleaseParticipant implements Checkpointing {

        final Handled handled = new Handled();
        final AtomicBoolean slowRelease = new AtomicBoolean();
        final AtomicBoolean releaseStarted = new AtomicBoolean();

        @EventHandler
        void on(String event, ProcessingContext context) {
            handled.record(event, segmentOf(context));
        }

        @Override
        public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment, TrackingToken requested) {
            return CompletableFuture.completedFuture(requested);
        }

        @Override
        public CompletableFuture<TrackingToken> onSegmentReleased(Segment segment, TrackingToken upTo) {
            if (segment.getSegmentId() != 0 || !slowRelease.get()) {
                return CompletableFuture.completedFuture(upTo);
            }
            releaseStarted.set(true);
            return CompletableFuture.supplyAsync(() -> upTo, CompletableFuture.delayedExecutor(
                    SLOW.toMillis(), TimeUnit.MILLISECONDS));
        }
    }

    @SequencingPolicy(type = FullConcurrencyPolicy.class)
    static final class PlainHandler {

        final Handled handled = new Handled();
        final AtomicBoolean slowBatchOnSegmentZero = new AtomicBoolean();
        final AtomicBoolean slowBatchStarted = new AtomicBoolean();

        @EventHandler
        void on(String event, ProcessingContext context) throws InterruptedException {
            int segment = segmentOf(context);
            handled.record(event, segment);
            if (segment == 0 && slowBatchOnSegmentZero.compareAndSet(true, false)) {
                slowBatchStarted.set(true);
                Thread.sleep(SLOW.toMillis());
            }
        }
    }
}
