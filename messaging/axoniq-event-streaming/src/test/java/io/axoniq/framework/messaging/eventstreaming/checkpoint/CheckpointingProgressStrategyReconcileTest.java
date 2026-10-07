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

import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.progress.SegmentProgressContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests how {@link CheckpointingProgressStrategy} combines the positions of 2 participants into 1 stored token,
 * during replay and on segment release, including participants that report no position.
 */
class CheckpointingProgressStrategyReconcileTest {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointingProgressStrategyReconcileTest.class);

    private static final TrackingToken G3 = new GlobalSequenceTrackingToken(3);
    private static final TrackingToken G10 = new GlobalSequenceTrackingToken(10);

    @Test
    void releaseOfUnconsumedReplaySegmentWithTwoParticipants() {
        TrackingToken unconsumed = ReplayToken.createReplayToken(G10);
        Outcome outcome = release(unconsumed, echo(), echo());
        logger.warn("release-unconsumed-replay token={} persisted={} failure={}", unconsumed, outcome.persisted,
                    outcome.failure);
        assertThat(outcome.failure).isNull();
    }

    @Test
    void releaseDuringReplayWithTwoParticipants() {
        TrackingToken replaying = ReplayToken.createReplayToken(G10, G3);
        Outcome outcome = release(replaying, echo(), echo());
        logger.warn("release-replaying token={} persisted={} failure={}", replaying, outcome.persisted,
                    outcome.failure);
        assertThat(outcome.failure).isNull();
        assertThat(outcome.persisted).containsExactly(replaying);
    }

    @Test
    void batchCheckpointDuringReplayWithTwoParticipants() {
        TrackingToken replaying = ReplayToken.createReplayToken(G10, G3);
        Outcome outcome = batch(replaying, TrackingToken.LATEST, false, echo(), echo());
        logger.warn("batch-replaying token={} persisted={} failure={}", replaying, outcome.persisted,
                    outcome.failure);
        assertThat(outcome.failure).isNull();
        assertThat(outcome.persisted).containsExactly(replaying);
    }

    @Test
    void autoModeBatchDuringReplay() {
        TrackingToken replaying = ReplayToken.createReplayToken(G10, G3);
        Outcome outcome = batch(replaying, replaying, true, echo());
        logger.warn("auto-replaying token={} persisted={} failure={}", replaying, outcome.persisted,
                    outcome.failure);
        assertThat(outcome.failure).isNull();
    }

    @Test
    void releaseWhereOneParticipantReportsNull() {
        Outcome outcome = release(G10, (s, t) -> CompletableFuture.completedFuture(null), echo());
        logger.warn("release-null-report persisted={} failure={}", outcome.persisted, outcome.failure);
        // a participant that reports nothing durable must not be passed by the stored token
        assertThat(outcome.persisted).isEmpty();
    }

    @Test
    void releaseWhereEveryParticipantReportsNull() {
        Outcome outcome = release(G10, (s, t) -> CompletableFuture.completedFuture(null),
                                  (s, t) -> CompletableFuture.completedFuture(null));
        logger.warn("release-all-null persisted={} failure={}", outcome.persisted, outcome.failure);
        assertThat(outcome.persisted).doesNotContain(TrackingToken.FIRST);
    }

    @Test
    void releaseWhereLaggardIsDrivenByCheckpointAdvanced() {
        // participant A is durable only up to 3 at release (allowed: best-effort), B reports 10
        List<String> calls = new CopyOnWriteArrayList<>();
        Checkpointing a = new Checkpointing() {
            @Override
            public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment, TrackingToken requested) {
                calls.add("A.advance(" + requested + ")");
                return CompletableFuture.failedFuture(new IllegalStateException("cannot reach " + requested));
            }

            @Override
            public CompletableFuture<TrackingToken> onSegmentReleased(Segment segment, TrackingToken upTo) {
                calls.add("A.release(" + upTo + ")");
                return CompletableFuture.completedFuture(G3);
            }
        };
        Outcome outcome = release(G10, List.of(a, participant(echo())));
        logger.warn("release-laggard calls={} persisted={} failure={}", calls, outcome.persisted,
                    outcome.failure);
        assertThat(outcome.persisted).containsExactly(G3);
    }

    // ---------------------------------------------------------------------------------------------------------------

    private static BiFunction<Segment, TrackingToken, CompletableFuture<TrackingToken>> echo() {
        return (s, t) -> CompletableFuture.completedFuture(t);
    }

    @SafeVarargs
    private static Outcome release(TrackingToken consumed,
                                   BiFunction<Segment, TrackingToken, CompletableFuture<TrackingToken>>... ps) {
        return release(consumed, java.util.Arrays.stream(ps).map(CheckpointingProgressStrategyReconcileTest::participant).toList());
    }

    private static Outcome release(TrackingToken consumed, List<Checkpointing> participants) {
        RecordingContext context = new RecordingContext(consumed);
        CheckpointingProgressStrategy strategy = new CheckpointingProgressStrategy(context, participants, false);
        return run(context, () -> strategy.onSegmentReleased(new StubProcessingContext()));
    }

    @SafeVarargs
    private static Outcome batch(TrackingToken consumed, TrackingToken request, boolean auto,
                                 BiFunction<Segment, TrackingToken, CompletableFuture<TrackingToken>>... ps) {
        RecordingContext context = new RecordingContext(consumed);
        List<Checkpointing> participants = java.util.Arrays.stream(ps)
                                                           .map(CheckpointingProgressStrategyReconcileTest::participant)
                                                           .toList();
        CheckpointingProgressStrategy strategy = new CheckpointingProgressStrategy(context, participants, auto);
        ProcessingContext pc = new StubProcessingContext();
        strategy.contributeBatchResources(pc);
        CheckpointTrigger.fromContext(pc).orElseThrow().requestCheckpoint(request);
        return run(context, () -> strategy.onBatchCommit(new StubProcessingContext()));
    }

    private static Outcome run(RecordingContext context, java.util.function.Supplier<CompletableFuture<Void>> action) {
        Throwable failure = null;
        try {
            action.get().join();
        } catch (Throwable e) {
            failure = e;
        }
        return new Outcome(context.persisted, failure);
    }

    private static Checkpointing participant(
            BiFunction<Segment, TrackingToken, CompletableFuture<TrackingToken>> advance) {
        return advance::apply;
    }

    private record Outcome(List<TrackingToken> persisted, @Nullable Throwable failure) {

    }

    private static final class RecordingContext implements SegmentProgressContext {

        private final TrackingToken consumed;
        private final List<TrackingToken> persisted = new CopyOnWriteArrayList<>();

        private RecordingContext(TrackingToken consumed) {
            this.consumed = consumed;
        }

        @Override
        public Segment segment() {
            return Segment.ROOT_SEGMENT;
        }

        @Override
        public @Nullable TrackingToken lastConsumedToken() {
            return consumed;
        }

        @Override
        public void scheduleWorker() {
        }

        @Override
        public CompletableFuture<Void> persistProgress(@Nullable TrackingToken candidate, ProcessingContext context) {
            if (candidate != null) {
                persisted.add(candidate);
            }
            return CompletableFuture.completedFuture(null);
        }
    }
}
