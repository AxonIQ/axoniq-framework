/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link WorkflowEngineCheckpointingSupport}.
 */
class WorkflowEngineCheckpointingSupportTest {

    @Test
    void waitsForTheInlineBarrierBeforeCompletingCheckpointAdvancement() {
        var coordinator = new InlineCheckpointLatchCoordinator();
        coordinator.pendingWork = true;
        var support = new WorkflowEngineCheckpointingSupport(coordinator);
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
        var support = new WorkflowEngineCheckpointingSupport(new InlineCheckpointLatchCoordinator());
        var requested = new GlobalSequenceTrackingToken(42);

        var result = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

        assertThat(result).isCompletedWithValue(requested);
    }

    @Test
    void retriesCheckpointBarrierWhenWorkAppearsAfterAnEmptyBarrierSchedule() {
        var coordinator = new ConcurrentWorkCoordinator();
        var support = new WorkflowEngineCheckpointingSupport(coordinator);
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
            var support = new WorkflowEngineCheckpointingSupport(coordinator);
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
            var support = new WorkflowEngineCheckpointingSupport(coordinator);

            // when
            var released = support.onSegmentReleased(Segment.ROOT_SEGMENT, new GlobalSequenceTrackingToken(42));

            // then
            assertThat(released).isCompletedExceptionally();
            assertThat(coordinator.scheduledBarriers).isZero();
        }
    }

    @Nested
    class WhileCheckpointWorkStaysUnsafeForManyLatchRounds {

        @Test
        void completesTheRequestOnceTheWorkTurnsSafeAfterManyRounds() {
            // given
            var coordinator = new PendingExecutionsCoordinator(1);
            var support = new WorkflowEngineCheckpointingSupport(coordinator);
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
            var support = new WorkflowEngineCheckpointingSupport(coordinator);
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
