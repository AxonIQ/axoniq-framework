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
import org.axonframework.messaging.eventhandling.processing.streaming.token.GapAwareTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class CheckpointingProgressStrategyPositionsTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    private static final TrackingToken AT_RESET = new GlobalSequenceTrackingToken(10);
    private static final TrackingToken REPLAYING_AT_3 =
            ReplayToken.createReplayToken(AT_RESET, new GlobalSequenceTrackingToken(3));
    private static final TrackingToken REPLAYING_AT_7 =
            ReplayToken.createReplayToken(AT_RESET, new GlobalSequenceTrackingToken(7));


    @Nested
    class DuringAReplay {

        @Test
        void batchCheckpointWithTwoParticipantsStoresTheReplayPosition() {
            // given
            var context = new RecordingProgressContext(REPLAYING_AT_3);
            var strategy = new CheckpointingProgressStrategy(context, List.of(echo(), echo()), false);
            requestCheckpoint(strategy, REPLAYING_AT_3);

            // when
            var result = strategy.onBatchCommit(new StubProcessingContext());

            // then
            assertThat(result).succeedsWithin(TIMEOUT);
            assertThat(context.persisted).containsExactly(REPLAYING_AT_3);
        }

        @Test
        void requestsArrivingWithinOneCycleStoreTheFurthestReplayPosition() {
            // given
            var context = new RecordingProgressContext(REPLAYING_AT_7);
            var strategy = new CheckpointingProgressStrategy(context, List.of(echo()), false);
            requestCheckpoint(strategy, REPLAYING_AT_3);
            requestCheckpoint(strategy, REPLAYING_AT_7);

            // when
            var result = strategy.onBatchCommit(new StubProcessingContext());

            // then
            assertThat(result).succeedsWithin(TIMEOUT);
            assertThat(context.persisted).containsExactly(REPLAYING_AT_7);
        }

        @Test
        void autoModeBatchStoresTheBatchEndReplayPosition() {
            // given
            var context = new RecordingProgressContext(REPLAYING_AT_7);
            var strategy = new CheckpointingProgressStrategy(context, List.of(echo()), true);
            requestCheckpoint(strategy, REPLAYING_AT_3);

            // when
            var result = strategy.onBatchCommit(new StubProcessingContext());

            // then
            assertThat(result).succeedsWithin(TIMEOUT);
            assertThat(context.persisted).containsExactly(REPLAYING_AT_7);
        }

        @Test
        void releaseWithTwoParticipantsStoresTheReplayPosition() {
            // given
            var context = new RecordingProgressContext(REPLAYING_AT_3);
            var strategy = new CheckpointingProgressStrategy(context, List.of(echo(), echo()), false);

            // when
            var result = strategy.onSegmentReleased(new StubProcessingContext());

            // then
            assertThat(result).succeedsWithin(TIMEOUT);
            assertThat(context.persisted).containsExactly(REPLAYING_AT_3);
        }

        @Test
        void releaseOfASegmentThatHandledNoEventYetKeepsItsReplayPosition() {
            // given a replay position without a current position: the segment has not handled an event since the reset
            var notStarted = ReplayToken.createReplayToken(AT_RESET);
            var context = new RecordingProgressContext(notStarted);
            var strategy = new CheckpointingProgressStrategy(context, List.of(echo(), echo()), false);

            // when
            var result = strategy.onSegmentReleased(new StubProcessingContext());

            // then
            assertThat(result).succeedsWithin(TIMEOUT);
            assertThat(context.persisted).containsExactly(notStarted);
        }
    }

    @Nested
    class OnRelease {

        private static final TrackingToken CONSUMED = new GlobalSequenceTrackingToken(10);

        @Test
        void aParticipantThatReportsNothingKeepsTheStoredToken() {
            // given
            var context = new RecordingProgressContext(CONSUMED);
            var strategy = new CheckpointingProgressStrategy(context, List.of(reportsNothing(), echo()), false);

            // when
            var result = strategy.onSegmentReleased(new StubProcessingContext());

            // then
            assertThat(result).succeedsWithin(TIMEOUT);
            assertThat(context.persisted).isEmpty();
        }

        @Test
        void everyParticipantReportingNothingStoresNothing() {
            // given
            var context = new RecordingProgressContext(CONSUMED);
            var strategy =
                    new CheckpointingProgressStrategy(context, List.of(reportsNothing(), reportsNothing()), false);

            // when
            var result = strategy.onSegmentReleased(new StubProcessingContext());

            // then
            assertThat(result).succeedsWithin(TIMEOUT);
            assertThat(context.persisted).isEmpty();
        }

        @Test
        void aParticipantThatCannotCatchUpLeadsToItsOwnReportedPosition() {
            // given a participant that is durable up to 3 at release and cannot advance further
            TrackingToken laggardPosition = new GlobalSequenceTrackingToken(3);
            Checkpointing laggard = new Checkpointing() {
                @Override
                public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment,
                                                                             TrackingToken requested) {
                    return CompletableFuture.failedFuture(new IllegalStateException("cannot reach " + requested));
                }

                @Override
                public CompletableFuture<TrackingToken> onSegmentReleased(Segment segment, TrackingToken upTo) {
                    return CompletableFuture.completedFuture(laggardPosition);
                }
            };
            var context = new RecordingProgressContext(CONSUMED);
            var strategy = new CheckpointingProgressStrategy(context, List.of(laggard, echo()), false);

            // when
            var result = strategy.onSegmentReleased(new StubProcessingContext());

            // then
            assertThat(result).succeedsWithin(TIMEOUT);
            assertThat(context.persisted).containsExactly(laggardPosition);
        }
    }

    @Nested
    class WithPositionsThatDoNotCoverEachOther {

        @Test
        void bothParticipantsAreDrivenToTheCombinedPosition() {
            // given one participant that is further ahead but still misses event 5, and one that saw event 5
            var requested = GapAwareTrackingToken.newInstance(4, Set.of());
            var aheadWithGap = GapAwareTrackingToken.newInstance(10, Set.of(5L));
            var behindWithoutGap = GapAwareTrackingToken.newInstance(7, Set.of());
            var context = new RecordingProgressContext(GapAwareTrackingToken.newInstance(10, Set.of()));
            var strategy = new CheckpointingProgressStrategy(
                    context, List.of(reportsAtLeast(aheadWithGap), reportsAtLeast(behindWithoutGap)), false
            );
            requestCheckpoint(strategy, requested);

            // when
            var result = strategy.onBatchCommit(new StubProcessingContext());

            // then neither position covers the other, so the stored token combines both
            assertThat(result).succeedsWithin(TIMEOUT);
            assertThat(context.persisted).containsExactly(GapAwareTrackingToken.newInstance(10, Set.of()));
        }

        private static Checkpointing reportsAtLeast(TrackingToken position) {
            return (segment, target) -> CompletableFuture.completedFuture(target.upperBound(position));
        }
    }

    @Nested
    class WhenAParticipantNeverAnswers {

        @Test
        void theCheckpointFailsInsteadOfWaitingForever() {
            // given
            var context = new RecordingProgressContext(new GlobalSequenceTrackingToken(1));
            Checkpointing neverAnswers = (segment, requested) -> new CompletableFuture<>();
            var strategy = new CheckpointingProgressStrategy(context, List.of(neverAnswers, echo()), false);
            requestCheckpoint(strategy, new GlobalSequenceTrackingToken(1));

            // when
            var commit = CompletableFuture.supplyAsync(() -> strategy.onBatchCommit(new StubProcessingContext()))
                                          .thenCompose(Function.identity());

            // then
            await().atMost(Duration.ofSeconds(45)).until(commit::isDone);
            assertThat(commit).isCompletedExceptionally();
            assertThat(context.persisted).isEmpty();
        }
    }

    private static Checkpointing echo() {
        return new Checkpointing() {
            @Override
            public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment, TrackingToken requested) {
                return CompletableFuture.completedFuture(requested);
            }
        };
    }

    private static Checkpointing reportsNothing() {
        return new Checkpointing() {
            @Override
            public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment, TrackingToken requested) {
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    private static void requestCheckpoint(CheckpointingProgressStrategy strategy, TrackingToken token) {
        ProcessingContext batchContext = new StubProcessingContext();
        strategy.contributeBatchResources(batchContext);
        CheckpointTrigger.fromContext(batchContext).orElseThrow().requestCheckpoint(token);
    }

    private static final class RecordingProgressContext implements SegmentProgressContext {

        private final TrackingToken consumed;
        private final List<TrackingToken> persisted = new CopyOnWriteArrayList<>();

        private RecordingProgressContext(TrackingToken consumed) {
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
