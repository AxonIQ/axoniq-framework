/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Tests for {@link WorkflowEngineCheckpointingSupport}.
 */
class WorkflowEngineCheckpointingSupportTest {

    @Test
    void waitsForTheInlineBarrierBeforeCompletingCheckpointAdvancement() {
        var coordinator = new InlineCheckpointLatchCoordinator();
        coordinator.pendingWork = true;
        var support = new WorkflowEngineCheckpointingSupport(coordinator, 0L);
        var requested = new GlobalSequenceTrackingToken(42);

        var result = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

        assertThat(result).isNotDone();
        assertThat(coordinator.barrier).isNotNull();

        coordinator.pendingWork = false;
        coordinator.crossBarrier();

        assertThat(result).isCompletedWithValue(requested);
    }

    @Test
    void completesImmediatelyWhenNoCheckpointWorkIsPending() {
        var support = new WorkflowEngineCheckpointingSupport(new InlineCheckpointLatchCoordinator(), 0L);
        var requested = new GlobalSequenceTrackingToken(42);

        var result = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

        assertThat(result).isCompletedWithValue(requested);
    }

    @Test
    void retriesCheckpointBarrierWhenWorkAppearsAfterAnEmptyBarrierSchedule() {
        var coordinator = new ConcurrentWorkCoordinator();
        var support = new WorkflowEngineCheckpointingSupport(coordinator, 0L);
        var requested = new GlobalSequenceTrackingToken(42);

        var result = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

        assertThat(result).isCompletedWithValue(requested);
        assertThat(coordinator.scheduledBarriers).isEqualTo(2);
    }

    @Nested
    class WhileTheCoordinatorHoldsTheCheckpoint {

        @Test
        void dropsACheckpointRequest() {
            // given
            var coordinator = new InlineCheckpointLatchCoordinator();
            coordinator.held = true;
            var support = new WorkflowEngineCheckpointingSupport(coordinator, 0L);
            var requests = new ArrayList<TrackingToken>();
            support.onSegmentClaimed(Segment.ROOT_SEGMENT, null, requests::add);

            // when
            support.requestCheckpoint(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(42));

            // then
            assertThat(requests).isEmpty();
        }

        @Test
        void failsTheReleaseSoTheStoredTokenStaysBehindTheHeldWork() {
            // given
            var coordinator = new InlineCheckpointLatchCoordinator();
            coordinator.held = true;
            var support = new WorkflowEngineCheckpointingSupport(coordinator, 0L);

            // when
            var released = support.onSegmentReleased(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(42));

            // then
            assertThat(released).isCompletedExceptionally();
            assertThat(coordinator.scheduledBarriers).isZero();
        }

        @Test
        void keepsFailingTheReleaseOnceTheHeldWorkIsGoneUntilTheSegmentIsClaimedAgain() {
            // given - a release that failed while the coordinator held the checkpoint
            var coordinator = new InlineCheckpointLatchCoordinator();
            coordinator.held = true;
            var support = new WorkflowEngineCheckpointingSupport(coordinator, 0L);
            support.onSegmentClaimed(Segment.ROOT_SEGMENT, null, token -> {
            });
            assertThat(support.onSegmentReleased(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(42)))
                    .isCompletedExceptionally();
            // and - the held work is dropped afterwards, as the release of its executions does
            coordinator.held = false;

            // when - the release is reported once more
            var releasedAgain = support.onSegmentReleased(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(42));

            // then
            assertThat(releasedAgain).isCompletedExceptionally();
            // and - a next claim of the segment releases normally again
            support.onSegmentClaimed(Segment.ROOT_SEGMENT, null, token -> {
            });
            assertThat(support.onSegmentReleased(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(43)))
                    .isCompletedWithValue(new GlobalSequenceTrackingToken(43));
        }
    }

    @Nested
    class WhileTheCoordinatorFlushes {

        private final FlushingCoordinator coordinator = new FlushingCoordinator();
        private final WorkflowEngineCheckpointingSupport support = new WorkflowEngineCheckpointingSupport(coordinator,
                                                                                                          0L);
        private final GlobalSequenceTrackingToken requested = new GlobalSequenceTrackingToken(42);

        @Test
        void completesTheCheckpointOnlyOnceTheFlushCompleted() {
            // when
            var checkpoint = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

            // then
            assertThat(checkpoint).isNotDone();

            // when
            coordinator.checkpointFlush.complete(null);

            // then
            assertThat(checkpoint).isCompletedWithValue(requested);
        }

        @Test
        void completesTheCheckpointOnlyOnceTheWorkflowWorkIsSafeAsWell() {
            // given
            coordinator.pendingWork = true;
            coordinator.checkpointFlush.complete(null);

            // when
            var checkpoint = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

            // then
            assertThat(checkpoint).isNotDone();

            // when
            coordinator.pendingWork = false;
            coordinator.barrier.run();

            // then
            assertThat(checkpoint).isCompletedWithValue(requested);
        }

        @Test
        void failsTheCheckpointWhenTheFlushFails() {
            // given
            coordinator.checkpointFlush.completeExceptionally(new IllegalStateException("database down"));

            // when
            var checkpoint = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

            // then
            assertThat(checkpoint).isCompletedExceptionally();
        }

        @Test
        void failsTheCheckpointRightAwayWhenTheFlushFailsWhileWorkflowWorkIsUnsafe() {
            // given
            coordinator.pendingWork = true;
            coordinator.checkpointFlush.completeExceptionally(new IllegalStateException("database down"));

            // when
            var checkpoint = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

            // then - it does not wait for the workflow work first
            assertThat(checkpoint).isCompletedExceptionally();
        }

        @Test
        void failsTheCheckpointWhenTheFlushThrows() {
            // given
            coordinator.checkpointFlush = null;

            // when
            var checkpoint = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

            // then
            assertThat(checkpoint).isCompletedExceptionally();
        }

        @Test
        void reportsTheReleasedTokenOnceTheReleaseFlushCompleted() {
            // when
            var released = support.onSegmentReleased(Segment.ROOT_SEGMENT, requested);

            // then - the release flushes through its own, bounded flush
            assertThat(released).isNotDone();
            coordinator.releaseFlush.complete(null);
            assertThat(released).isCompletedWithValue(requested);
        }

        @Test
        void failsTheReleaseWhenTheReleaseFlushFails() {
            // given
            coordinator.releaseFlush.completeExceptionally(new IllegalStateException("database down"));

            // when
            var released = support.onSegmentReleased(Segment.ROOT_SEGMENT, requested);

            // then - the release does not advance the stored token
            assertThat(released).isCompletedExceptionally();
        }
    }

    @Nested
    class WithACheckpointInterval {

        private final List<TrackingToken> requests = new ArrayList<>();

        @Test
        void forwardsEveryRequestWithoutAnInterval() {
            // given
            var support = new WorkflowEngineCheckpointingSupport(new InlineCheckpointLatchCoordinator(), 0L);
            support.onSegmentClaimed(Segment.ROOT_SEGMENT, null, requests::add);

            // when
            support.requestCheckpoint(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(1));
            support.requestCheckpoint(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(2));

            // then
            assertThat(requests).containsExactly(new GlobalSequenceTrackingToken(1),
                                                 new GlobalSequenceTrackingToken(2));
        }

        @Test
        void forwardsOnlyTheFirstRequestWithinTheInterval() {
            // given
            var support = new WorkflowEngineCheckpointingSupport(new InlineCheckpointLatchCoordinator(),
                                                                 Duration.ofHours(1).toMillis());
            support.onSegmentClaimed(Segment.ROOT_SEGMENT, null, requests::add);

            // when
            support.requestCheckpoint(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(1));
            support.requestCheckpoint(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(2));

            // then
            assertThat(requests).containsExactly(new GlobalSequenceTrackingToken(1));
        }

        @Test
        void forwardsTheNextRequestOnceTheIntervalPassed() {
            // given
            var support = new WorkflowEngineCheckpointingSupport(new InlineCheckpointLatchCoordinator(), 20L);
            support.onSegmentClaimed(Segment.ROOT_SEGMENT, null, requests::add);
            support.requestCheckpoint(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(1));

            // when / then
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                support.requestCheckpoint(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(2));
                assertThat(requests).contains(new GlobalSequenceTrackingToken(2));
            });
        }

        @Test
        void keepsTheIntervalPerSegment() {
            // given
            var support = new WorkflowEngineCheckpointingSupport(new InlineCheckpointLatchCoordinator(),
                                                                 Duration.ofHours(1).toMillis());
            Segment[] halves = Segment.ROOT_SEGMENT.split();
            support.onSegmentClaimed(halves[0], null, requests::add);
            support.onSegmentClaimed(halves[1], null, requests::add);

            // when
            support.requestCheckpoint(halves[0], new GlobalSequenceTrackingToken(1));
            support.requestCheckpoint(halves[1], new GlobalSequenceTrackingToken(2));

            // then
            assertThat(requests).containsExactly(new GlobalSequenceTrackingToken(1),
                                                 new GlobalSequenceTrackingToken(2));
        }

        @Test
        void forwardsTheFirstRequestOfASegmentClaimedAgainAfterItsRelease() {
            // given
            var support = new WorkflowEngineCheckpointingSupport(new InlineCheckpointLatchCoordinator(),
                                                                 Duration.ofHours(1).toMillis());
            support.onSegmentClaimed(Segment.ROOT_SEGMENT, null, requests::add);
            support.requestCheckpoint(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(1));
            support.onSegmentReleased(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(1));
            support.onSegmentClaimed(Segment.ROOT_SEGMENT, null, requests::add);

            // when
            support.requestCheckpoint(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(2));

            // then
            assertThat(requests).containsExactly(new GlobalSequenceTrackingToken(1),
                                                 new GlobalSequenceTrackingToken(2));
        }

        @Test
        void rejectsANegativeInterval() {
            // given
            var coordinator = new InlineCheckpointLatchCoordinator();

            // when / then
            assertThatThrownBy(() -> new WorkflowEngineCheckpointingSupport(coordinator, -1L))
                    .isInstanceOf(AxonConfigurationException.class);
        }
    }

    @Nested
    class WhileCheckpointWorkStaysUnsafeForManyLatchRounds {

        @Test
        void completesTheRequestOnceTheWorkTurnsSafeAfterManyRounds() {
            // given
            var coordinator = new PendingExecutionsCoordinator(1);
            var support = new WorkflowEngineCheckpointingSupport(coordinator, 0L);
            var requested = new GlobalSequenceTrackingToken(42);
            var result = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);
            // Large enough that completing one nested future per round would overflow the stack.
            for (int round = 0; round < 100_000; round++) {
                coordinator.crossPendingLatches();
            }
            assertThat(result).isNotDone();

            // when
            coordinator.pendingWork = false;
            coordinator.crossPendingLatches();

            // then
            assertThat(result).isCompletedWithValue(requested);
        }

        @Test
        void schedulesOneFollowUpLatchPerCrossingWhenSeveralExecutionsShareALatch() {
            // given
            int executions = 8;
            int rounds = 2;
            var coordinator = new PendingExecutionsCoordinator(executions);
            var support = new WorkflowEngineCheckpointingSupport(coordinator, 0L);
            var requested = new GlobalSequenceTrackingToken(42);
            var result = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

            // when
            for (int round = 0; round < rounds; round++) {
                coordinator.crossPendingLatches();
            }

            // then the initial latch plus at most one follow-up per execution crossing, not one per shared callback
            assertThat(coordinator.scheduledLatches).isLessThanOrEqualTo(1 + rounds * executions);
            assertThat(result).isNotDone();
        }

        @Test
        void failsTheRequestWhenAFollowUpLatchCannotBeAttached() {
            // given
            var coordinator = new PendingExecutionsCoordinator(2);
            var support = new WorkflowEngineCheckpointingSupport(coordinator, 0L);
            var result = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(42));
            var failure = new IllegalStateException("execution queue closed");
            coordinator.latchFailure = failure;

            // when crossing the latch re-checks, finds the work still unsafe and fails to attach the follow-up
            coordinator.crossPendingLatches();

            // then
            assertThat(result).failsWithin(Duration.ZERO)
                              .withThrowableThat()
                              .withCause(failure);
        }
    }

    /**
     * Attaches every latch to a fixed set of unsafe executions, which cross their pending latches one execution at a
     * time from a flat loop, like workflow threads draining their queues.
     */
    private static final class PendingExecutionsCoordinator
            implements WorkflowEngineCheckpointingSupport.CheckpointLatchCoordinator {

        private final List<List<Runnable>> pendingLatchesPerExecution = new ArrayList<>();
        private boolean pendingWork = true;
        private int scheduledLatches;
        private RuntimeException latchFailure;

        private PendingExecutionsCoordinator(int executions) {
            for (int i = 0; i < executions; i++) {
                pendingLatchesPerExecution.add(new ArrayList<>());
            }
        }

        @Override
        public boolean hasUnsafeCheckpointWork(Segment segment) {
            return pendingWork;
        }

        @Override
        public boolean holdsCheckpoint(Segment segment) {
            return false;
        }

        @Override
        public void addCheckpointLatch(Segment segment, Runnable latch) {
            if (latchFailure != null) {
                throw latchFailure;
            }
            scheduledLatches++;
            pendingLatchesPerExecution.forEach(pending -> pending.add(latch));
        }

        private void crossPendingLatches() {
            for (int i = 0; i < pendingLatchesPerExecution.size(); i++) {
                List<Runnable> crossed = pendingLatchesPerExecution.set(i, new ArrayList<>());
                crossed.forEach(Runnable::run);
            }
        }
    }

    private static final class InlineCheckpointLatchCoordinator
            implements WorkflowEngineCheckpointingSupport.CheckpointLatchCoordinator {

        private boolean pendingWork;
        private boolean held;
        private Runnable barrier;
        private int scheduledBarriers;

        @Override
        public boolean hasUnsafeCheckpointWork(Segment segment) {
            return pendingWork;
        }

        @Override
        public boolean holdsCheckpoint(Segment segment) {
            return held;
        }

        @Override
        public void addCheckpointLatch(Segment segment, Runnable latch) {
            barrier = latch;
            scheduledBarriers++;
        }

        private void crossBarrier() {
            barrier.run();
        }
    }

    /**
     * A coordinator whose flushes complete when the test completes them, and whose unsafe work stays unsafe until the
     * test clears it and crosses the barrier.
     */
    private static final class FlushingCoordinator
            implements WorkflowEngineCheckpointingSupport.CheckpointLatchCoordinator {

        private boolean pendingWork;
        private Runnable barrier;
        private CompletableFuture<Void> checkpointFlush = new CompletableFuture<>();
        private final CompletableFuture<Void> releaseFlush = new CompletableFuture<>();

        @Override
        public boolean hasUnsafeCheckpointWork(Segment segment) {
            return pendingWork;
        }

        @Override
        public boolean holdsCheckpoint(Segment segment) {
            return false;
        }

        @Override
        public void addCheckpointLatch(Segment segment, Runnable latch) {
            barrier = latch;
        }

        @Override
        public CompletableFuture<Void> flushCheckpoint(Segment segment, @Nullable ProcessingContext context) {
            if (checkpointFlush == null) {
                throw new IllegalStateException("cannot flush");
            }
            return checkpointFlush;
        }

        @Override
        public CompletableFuture<Void> flushRelease(Segment segment) {
            return releaseFlush;
        }
    }

    private static final class ConcurrentWorkCoordinator
            implements WorkflowEngineCheckpointingSupport.CheckpointLatchCoordinator {

        private boolean workAppended;
        private int scheduledBarriers;

        @Override
        public boolean hasUnsafeCheckpointWork(Segment segment) {
            return scheduledBarriers == 0 || workAppended;
        }

        @Override
        public boolean holdsCheckpoint(Segment segment) {
            return false;
        }

        @Override
        public void addCheckpointLatch(Segment segment, Runnable latch) {
            scheduledBarriers++;
            // The first schedule sees an empty snapshot. A workflow thread appends work before the callback re-checks.
            workAppended = scheduledBarriers == 1;
            latch.run();
        }
    }
}
