/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

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
