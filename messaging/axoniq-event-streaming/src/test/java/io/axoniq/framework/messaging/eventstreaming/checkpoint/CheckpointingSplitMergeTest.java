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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests that segment split and merge run the checkpoint release protocol before repartitioning: the
 * {@link Checkpointing} participant's release hook fires, and its final reconciled position is persisted
 * <em>before</em> the split or merge reads the segment's token. A fully-deferred projection therefore hands its
 * drained position to the new segment layout instead of the (lagging) last stored checkpoint.
 */
class CheckpointingSplitMergeTest {

    private static final String PROCESSOR_NAME = "split-merge";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private AxonConfiguration configuration;

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Test
    void splitPersistsTheReleaseFlushedPositionAndStartsBothHalvesFromIt() {
        // given -- a lone deferred projection whose stored checkpoint lags behind what it handled
        AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource();
        InMemoryTokenStore tokenStore = new InMemoryTokenStore();
        RecordingProjection projection = new RecordingProjection();
        start(eventSource, tokenStore, projection, 1);

        for (int i = 0; i < 5; i++) {
            eventSource.publishMessage(EventTestUtils.asEventMessage("event-" + i));
        }
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(projection.handled).hasSize(5));
        long handledPosition = positionOf(projection.handledTokens.get(4));

        projection.trigger.requestCheckpoint(projection.handledTokens.get(1));
        long checkpointedPosition = positionOf(projection.handledTokens.get(1));
        await().atMost(TIMEOUT).untilAsserted(
                () -> assertThat(storedPosition(tokenStore, 0)).isEqualTo(checkpointedPosition)
        );

        // when -- the segment is split
        Boolean split = processor().splitSegment(0).orTimeout(5, TimeUnit.SECONDS).join();

        // then -- the release hook fired, and both halves start at the release-flushed position, not the old checkpoint
        assertThat(split).isTrue();
        assertThat(projection.released).extracting(Segment::getSegmentId).contains(0);
        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(storedPosition(tokenStore, 0)).isEqualTo(handledPosition);
            assertThat(storedPosition(tokenStore, 1)).isEqualTo(handledPosition);
        });
    }

    @Test
    void mergePersistsBothReleaseFlushedPositionsBeforeCombiningThem() {
        // given -- two segments, each consumed to the stream end, with no checkpoint ever requested
        AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource();
        InMemoryTokenStore tokenStore = new InMemoryTokenStore();
        RecordingProjection projection = new RecordingProjection();
        start(eventSource, tokenStore, projection, 2);

        for (int i = 0; i < 5; i++) {
            eventSource.publishMessage(EventTestUtils.asEventMessage("event-" + i));
        }
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(projection.handled).hasSize(5));
        long handledPosition = projection.handledTokens.stream()
                                                       .mapToLong(CheckpointingSplitMergeTest::positionOf)
                                                       .max()
                                                       .orElseThrow();

        // when -- the segments are merged
        Boolean merged = processor().mergeSegment(0).orTimeout(5, TimeUnit.SECONDS).join();

        // then -- both segments were released through the checkpoint protocol, and the merged token combines the
        // release-flushed positions (the consumed stream end), not the never-advanced stored tokens
        assertThat(merged).isTrue();
        assertThat(projection.released).extracting(Segment::getSegmentId).containsExactlyInAnyOrder(0, 1);
        await().atMost(TIMEOUT).untilAsserted(
                () -> assertThat(storedPosition(tokenStore, 0)).isEqualTo(handledPosition)
        );
    }

    private void start(AsyncInMemoryStreamableEventSource eventSource,
                       InMemoryTokenStore tokenStore,
                       RecordingProjection projection,
                       int segmentCount) {
        var module = EventProcessorModule.pooledStreaming(PROCESSOR_NAME)
                                         .eventHandlingComponents(components -> components.autodetected(
                                                 "projection", cfg -> projection
                                         ))
                                         .customized((cfg, c) -> c.eventSource(eventSource)
                                                                  .tokenStore(tokenStore)
                                                                  .initialSegmentCount(segmentCount));
        configuration = MessagingConfigurer.create()
                                           .eventProcessing(ep -> ep.pooledStreaming(ps -> ps.processor(module)))
                                           .build();
        configuration.start();
    }

    private PooledStreamingEventProcessor processor() {
        return (PooledStreamingEventProcessor) configuration.getComponents(EventProcessor.class)
                                                            .get(PROCESSOR_NAME);
    }

    private static long storedPosition(TokenStore tokenStore, int segmentId) {
        TrackingToken token = FutureUtils.joinAndUnwrap(tokenStore.fetchToken(PROCESSOR_NAME, segmentId, null));
        return token == null ? -1L : token.position().orElse(-1L);
    }

    private static long positionOf(TrackingToken token) {
        return token.position().orElse(-1L);
    }

    /**
     * A fully-deferred projection recording what it handled, what it was asked to advance to, and which segments were
     * released. Confirms durability immediately on {@link #onCheckpointAdvanced}, so the release flush drains it to the
     * consumed position.
     */
    static class RecordingProjection implements Checkpointing {

        private final List<String> handled = new CopyOnWriteArrayList<>();
        private final List<TrackingToken> handledTokens = new CopyOnWriteArrayList<>();
        private final List<Segment> released = new CopyOnWriteArrayList<>();
        private volatile CheckpointTrigger trigger;

        @EventHandler
        void on(String event, TrackingToken token, CheckpointTrigger checkpoint) {
            this.trigger = checkpoint;
            handledTokens.add(token);
            handled.add(event);
        }

        @Override
        public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment, TrackingToken requested) {
            return CompletableFuture.completedFuture(requested);
        }

        @Override
        public CompletableFuture<TrackingToken> onSegmentReleased(Segment segment, TrackingToken upTo) {
            released.add(segment);
            return Checkpointing.super.onSegmentReleased(segment, upTo);
        }
    }
}
