/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.RunningWorkflowsClaimLoader.ClaimedIndex;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.framework.workflow.runtime.execution.LifecycleEventFixtures.lifecycleEvent;
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
        void writesOnlyOnceTheWorkflowWorkIsSafe() {
            // given
            coordinator.pendingWork = true;
            coordinator.checkpointFlush.complete(null);

            // when
            var checkpoint = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

            // then
            assertThat(coordinator.writes).isEmpty();

            // when
            coordinator.pendingWork = false;
            coordinator.barrier.run();

            // then
            assertThat(coordinator.writes).containsExactly(new Write(null, false));
            assertThat(checkpoint).isCompletedWithValue(requested);
        }

        @Test
        void failsTheCheckpointRightAwayWhenDecidingWhatToWriteFails() {
            // given
            coordinator.pendingWork = true;
            coordinator.checkpointFlush = null;

            // when
            var checkpoint = support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

            // then - it does not wait for the workflow work first
            assertThat(checkpoint).isCompletedExceptionally();
            assertThat(coordinator.writes).isEmpty();
        }

        @Test
        void writesWithinTheContextOnTheCallingThreadOnlyOnceTheWorkflowWorkIsSafe() {
            // given
            coordinator.pendingWork = true;
            coordinator.checkpointFlush.complete(null);
            var context = new StubProcessingContext();
            var processorThread = new AtomicReference<Thread>();

            // when - the processor takes the checkpoint on its own thread, which waits for the workflow work
            var checkpoint = CompletableFuture.supplyAsync(() -> {
                processorThread.set(Thread.currentThread());
                return support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested, context);
            }).thenCompose(advanced -> advanced);
            await().atMost(Duration.ofSeconds(5)).until(() -> support.safePointWaiters.get() == 1);

            // then - nothing is written within the transaction of the processor while the workflows are not safe
            assertThat(coordinator.writes).isEmpty();
            assertThat(checkpoint).isNotDone();

            // when
            coordinator.pendingWork = false;
            coordinator.barrier.run();

            // then - written within the context, on the thread taking the checkpoint, after the safe point
            assertThat(checkpoint).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(requested);
            assertThat(coordinator.writes).containsExactly(new Write(context, false));
            assertThat(coordinator.writeThreads).containsExactly(processorThread.get());
        }

        @Test
        void failsTheCheckpointWithoutWritingWhenTheWorkflowWorkDoesNotTurnSafeInTime() {
            // given
            var impatient = new WorkflowEngineCheckpointingSupport(coordinator, 0L, Duration.ofMillis(100));
            coordinator.pendingWork = true;
            coordinator.checkpointFlush.complete(null);

            // when
            var checkpoint = impatient.onCheckpointAdvanced(Segment.ROOT_SEGMENT,
                                                            requested,
                                                            new StubProcessingContext());

            // then - the decided state stays to be written by a later checkpoint
            assertThat(checkpoint).isCompletedExceptionally();
            assertThat(coordinator.writes).isEmpty();
        }

        @Test
        void failsTheCheckpointWithoutAContextWithoutWritingWhenTheWorkflowWorkDoesNotTurnSafeInTime() {
            // given
            var impatient = new WorkflowEngineCheckpointingSupport(coordinator, 0L, Duration.ofMillis(100));
            coordinator.pendingWork = true;
            coordinator.checkpointFlush.complete(null);

            // when
            var checkpoint = impatient.onCheckpointAdvanced(Segment.ROOT_SEGMENT, requested);

            // then
            assertThat(checkpoint).failsWithin(Duration.ofSeconds(5))
                                  .withThrowableOfType(ExecutionException.class)
                                  .withCauseInstanceOf(TimeoutException.class);

            // when - the workflow work turns safe after the checkpoint was given up on
            coordinator.pendingWork = false;
            coordinator.barrier.run();

            // then - the decided state stays to be written by a later checkpoint
            assertThat(coordinator.writes).isEmpty();
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
    class WithTheRunningWorkflowsProjection {

        private static final String INDEX = "orders";

        private final ControllableWorkflowIndex workflowIndex =
                new ControllableWorkflowIndex().initialized(INDEX).completeWritesImmediately();
        private final RunningWorkflowsProjection projection = new RunningWorkflowsProjection(workflowIndex, INDEX);
        private final ProjectingCoordinator coordinator = new ProjectingCoordinator(projection);

        /**
         * Takes a checkpoint at the given {@code token} within a context, as the processor does, and returns its
         * outcome.
         */
        private CompletableFuture<TrackingToken> checkpointWithinAContext(WorkflowEngineCheckpointingSupport support,
                                                                          TrackingToken token) {
            var checkpoint = new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE).create();
            var advanced = new AtomicReference<CompletableFuture<TrackingToken>>();
            checkpoint.runOnPrepareCommit(
                    context -> advanced.set(support.onCheckpointAdvanced(Segment.ROOT_SEGMENT, token, context))
            );
            checkpoint.execute().orTimeout(5, TimeUnit.SECONDS).join();
            return advanced.get();
        }

        @Test
        void writesWhatACheckpointThatTimedOutDecidedWithTheNextCheckpoint() {
            // given - a started workflow of the segment
            projection.initialize(Segment.ROOT_SEGMENT, new ClaimedIndex(Set.of(), Map.of()));
            projection.apply(Segment.ROOT_SEGMENT, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            var support = new WorkflowEngineCheckpointingSupport(coordinator, 0L, Duration.ofMillis(100));
            // and - a checkpoint whose workflow work did not turn safe in time
            coordinator.pendingWork = true;
            var timedOut = checkpointWithinAContext(support, new GlobalSequenceTrackingToken(42));
            assertThat(timedOut).isCompletedExceptionally();
            assertThat(workflowIndex.issuedWrites()).isEmpty();

            // when - the workflow work is safe for the next checkpoint
            coordinator.pendingWork = false;
            var next = checkpointWithinAContext(support, new GlobalSequenceTrackingToken(43));

            // then - it writes what the checkpoint that timed out decided
            assertThat(next).isCompletedWithValue(new GlobalSequenceTrackingToken(43));
            assertThat(workflowIndex.durableEntries(INDEX, Segment.ROOT_SEGMENT))
                    .containsExactly(WorkflowIndex.Entry.running("order-1"));
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
    class WithASafePointTimeout {

        @Test
        void rejectsATimeoutThatIsNotPositive() {
            // given
            var coordinator = new InlineCheckpointLatchCoordinator();

            // when / then
            assertThatThrownBy(() -> new WorkflowEngineCheckpointingSupport(coordinator, 0L, Duration.ZERO))
                    .isInstanceOf(AxonConfigurationException.class);
            assertThatThrownBy(() -> new WorkflowEngineCheckpointingSupport(coordinator, 0L, Duration.ofSeconds(-1)))
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
     * A checkpoint write of a {@link FlushingCoordinator}: the context it was given, and whether the workflow work was
     * unsafe when it was written.
     */
    private record Write(@Nullable ProcessingContext context, boolean whileUnsafe) {

    }

    /**
     * A coordinator whose flushes complete when the test completes them, and whose unsafe work stays unsafe until the
     * test clears it and crosses the barrier. It records every checkpoint write, and the thread writing it.
     */
    private static final class FlushingCoordinator
            implements WorkflowEngineCheckpointingSupport.CheckpointLatchCoordinator {

        private volatile boolean pendingWork;
        private volatile Runnable barrier;
        private volatile CompletableFuture<Void> checkpointFlush = new CompletableFuture<>();
        private final CompletableFuture<Void> releaseFlush = new CompletableFuture<>();
        private final List<Write> writes = new CopyOnWriteArrayList<>();
        private final List<Thread> writeThreads = new CopyOnWriteArrayList<>();

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
        public WorkflowEngineCheckpointingSupport.CheckpointFlush decideCheckpointFlush(Segment segment) {
            var flush = checkpointFlush;
            if (flush == null) {
                throw new IllegalStateException("cannot flush");
            }
            return context -> {
                writes.add(new Write(context, pendingWork));
                writeThreads.add(Thread.currentThread());
                return flush;
            };
        }

        @Override
        public CompletableFuture<Void> flushRelease(Segment segment) {
            return releaseFlush;
        }
    }

    /**
     * A coordinator deciding its checkpoint writes through a real {@link RunningWorkflowsProjection}, whose workflow
     * work is unsafe while the test says so.
     */
    private static final class ProjectingCoordinator
            implements WorkflowEngineCheckpointingSupport.CheckpointLatchCoordinator {

        private final RunningWorkflowsProjection projection;
        private volatile boolean pendingWork;

        private ProjectingCoordinator(RunningWorkflowsProjection projection) {
            this.projection = projection;
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
            // The workflow work never reaches this latch: it only turns safe for a later checkpoint.
        }

        @Override
        public WorkflowEngineCheckpointingSupport.CheckpointFlush decideCheckpointFlush(Segment segment) {
            return projection.decideCheckpoint(segment);
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
