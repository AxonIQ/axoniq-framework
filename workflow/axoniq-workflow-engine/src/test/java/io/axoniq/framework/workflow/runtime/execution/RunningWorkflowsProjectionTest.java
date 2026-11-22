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
import io.axoniq.framework.workflow.index.api.WorkflowIndex.Entry;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.IndexDelta;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.OpenWait;
import io.axoniq.framework.workflow.runtime.execution.RunningWorkflowsClaimLoader.ClaimedIndex;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.axoniq.framework.workflow.runtime.execution.LifecycleEventFixtures.*;
import static io.axoniq.framework.workflow.runtime.execution.SegmentTestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RunningWorkflowsProjection}.
 *
 * @author Allard Buijze
 */
class RunningWorkflowsProjectionTest {

    private static final String INDEX = "orders";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final String LONG_ID = "w".repeat(2_000);

    private final ControllableWorkflowIndex workflowIndex = new ControllableWorkflowIndex();
    private final RunningWorkflowsProjection projection = new RunningWorkflowsProjection(workflowIndex, INDEX);

    private void claim(Segment segment) {
        claim(segment, Map.of());
    }

    private void claim(Segment segment, Map<String, Boolean> changes) {
        projection.initialize(segment, new ClaimedIndex(Set.of(), changes));
    }

    private static UnitOfWork unitOfWork() {
        return new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE).create();
    }

    private Set<String> stored(Segment segment) {
        return await(workflowIndex.load(INDEX, segment)).orElseThrow()
                                                        .stream()
                                                        .map(WorkflowIndex.Entry::workflowId)
                                                        .collect(Collectors.toSet());
    }

    private static <T> T await(CompletableFuture<T> future) {
        return future.orTimeout(5, TimeUnit.SECONDS).join();
    }

    private static Throwable failureOf(CompletableFuture<?> future) {
        return assertThat(future).failsWithin(Duration.ofSeconds(5))
                                 .withThrowableOfType(ExecutionException.class)
                                 .actual()
                                 .getCause();
    }

    @Nested
    class Apply {

        private final Segment segment = FOUR_SEGMENTS.getFirst();
        private final String workflowId = anyIdOn(segment);

        @BeforeEach
        void setUp() {
            workflowIndex.completeWritesImmediately();
            claim(segment);
        }

        @Test
        void startedEventAddsTheWorkflow() {
            // when
            projection.apply(segment, lifecycleEvent(workflowId, WorkflowStatus.STARTED));
            await(projection.flushForCheckpoint(segment, null));

            // then
            assertThat(workflowIndex.issuedWrites()).singleElement().satisfies(
                    write -> assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of(workflowId), Set.of()))
            );
        }

        @ParameterizedTest
        @EnumSource(value = WorkflowStatus.class, names = {"COMPLETED", "FAILED", "TIMED_OUT", "CANCELLED"})
        void terminalEventRemovesTheWorkflow(WorkflowStatus terminal) {
            // given
            projection.apply(segment, lifecycleEvent(workflowId, WorkflowStatus.STARTED));

            // when
            projection.apply(segment, lifecycleEvent(workflowId, terminal));
            await(projection.flushForCheckpoint(segment, null));

            // then - only the latest state of the workflow is written
            assertThat(workflowIndex.issuedWrites()).singleElement().satisfies(
                    write -> assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of(), Set.of(workflowId)))
            );
        }

        @Test
        void eventsThatChangeNoMembershipWriteNothing() {
            // when
            projection.apply(segment, lifecycleEvent(workflowId, WorkflowStatus.NONE));
            projection.apply(segment, stepEvent(workflowId));
            await(projection.flushForCheckpoint(segment, null));

            // then
            assertThat(workflowIndex.issuedWrites()).isEmpty();
        }

        @Test
        void eventPublishedThroughThePublishPrimitiveIsIgnored() {
            // when
            projection.apply(segment, publishedEvent(workflowId, WorkflowStatus.STARTED));
            await(projection.flushForCheckpoint(segment, null));

            // then
            assertThat(workflowIndex.issuedWrites()).isEmpty();
        }

        @Test
        void eventOfAWorkflowOwnedByAnotherSegmentIsIgnored() {
            // given
            var foreignId = anyIdOn(anotherSegmentThan(segment));

            // when
            projection.apply(segment, lifecycleEvent(foreignId, WorkflowStatus.STARTED));
            await(projection.flushForCheckpoint(segment, null));

            // then
            assertThat(workflowIndex.issuedWrites()).isEmpty();
        }

        @Test
        void eventOfASegmentThatIsNotTrackedIsIgnored() {
            // given
            var untracked = anotherSegmentThan(segment);

            // when
            projection.apply(untracked, lifecycleEvent(anyIdOn(untracked), WorkflowStatus.STARTED));
            await(projection.flushForCheckpoint(segment, null));

            // then
            assertThat(workflowIndex.issuedWrites()).isEmpty();
        }
    }

    @Nested
    class Reapplication {

        @Test
        void reapplyingASuffixOfTheStreamConvergesToTheSameRunningSet() {
            // given
            workflowIndex.initialized(INDEX).completeWritesImmediately();
            claim(Segment.ROOT_SEGMENT);
            EventMessage[] stream = {
                    lifecycleEvent("order-1", WorkflowStatus.STARTED),
                    lifecycleEvent("order-2", WorkflowStatus.STARTED),
                    lifecycleEvent("order-1", WorkflowStatus.COMPLETED),
                    lifecycleEvent("order-3", WorkflowStatus.STARTED),
                    lifecycleEvent("order-2", WorkflowStatus.FAILED),
            };
            handle(stream, 0);
            var afterStream = stored(Segment.ROOT_SEGMENT);

            // when - the suffix from the second event is handled again, as after a crash before the token was stored
            handle(stream, 1);

            // then
            assertThat(stored(Segment.ROOT_SEGMENT)).isEqualTo(afterStream).containsExactly("order-3");
        }

        private void handle(EventMessage[] stream, int from) {
            for (int i = from; i < stream.length; i++) {
                projection.apply(Segment.ROOT_SEGMENT, stream[i]);
                await(projection.flushForCheckpoint(Segment.ROOT_SEGMENT, null));
            }
        }
    }

    @Nested
    class FlushForCheckpoint {

        private final Segment segment = Segment.ROOT_SEGMENT;

        @BeforeEach
        void setUp() {
            workflowIndex.initialized(INDEX);
        }

        @Test
        void completesRightAwayWhenNothingIsPending() {
            // given - a quiet segment, handling events that change no running workflow
            claim(segment);
            projection.apply(segment, stepEvent("order-1"));

            // when
            var checkpoint = projection.flushForCheckpoint(segment, null);

            // then
            assertThat(checkpoint).isCompleted();
            assertThat(workflowIndex.issuedWrites()).isEmpty();
        }

        @Test
        void writesNothingBeforeACheckpointIsTaken() {
            // given
            claim(segment, Map.of("order-1", true));

            // when
            projection.apply(segment, lifecycleEvent("order-2", WorkflowStatus.STARTED));

            // then
            assertThat(workflowIndex.issuedWrites()).isEmpty();
        }

        @Test
        void completesOnlyOnceEveryChangeHandledBeforeItIsDurable() {
            // given - a change the claim found, and one handled since
            claim(segment, Map.of("order-1", true));
            projection.apply(segment, lifecycleEvent("order-2", WorkflowStatus.STARTED));

            // when
            var checkpoint = projection.flushForCheckpoint(segment, null);

            // then - one write carries every pending change, and the checkpoint waits for it
            assertThat(workflowIndex.pendingWrites()).singleElement().satisfies(
                    write -> assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of("order-1", "order-2"),
                                                                                Set.of()))
            );
            assertThat(checkpoint).isNotDone();

            // when
            workflowIndex.completeNextWrite();

            // then
            await(checkpoint);
            assertThat(stored(segment)).containsExactlyInAnyOrder("order-1", "order-2");
        }

        @Test
        void waitsForAWriteInFlightBeforeWritingTheChangesHandledSince() {
            // given - a write of an earlier checkpoint that is still in flight
            claim(segment);
            projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            projection.checkpointTimeout = Duration.ofMillis(10);
            failureOf(projection.flushForCheckpoint(segment, null));
            projection.checkpointTimeout = TIMEOUT;
            projection.apply(segment, lifecycleEvent("order-2", WorkflowStatus.STARTED));

            // when
            var checkpoint = projection.flushForCheckpoint(segment, null);

            // then - the index sees one write at a time
            assertThat(workflowIndex.issuedWrites()).hasSize(1);

            // when
            workflowIndex.completeNextWrite();

            // then - the changes handled since follow, and the checkpoint waits for them
            assertThat(workflowIndex.pendingWrites()).singleElement().satisfies(
                    write -> assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of("order-2"), Set.of()))
            );
            assertThat(checkpoint).isNotDone();
            workflowIndex.completeNextWrite();
            await(checkpoint);
            assertThat(stored(segment)).containsExactlyInAnyOrder("order-1", "order-2");
        }

        @Test
        void rewritesTheChangesOfAFailedWriteInFlight() {
            // given - a write of an earlier checkpoint that is still in flight
            claim(segment);
            projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            projection.checkpointTimeout = Duration.ofMillis(10);
            failureOf(projection.flushForCheckpoint(segment, null));
            projection.checkpointTimeout = TIMEOUT;
            var checkpoint = projection.flushForCheckpoint(segment, null);

            // when - that write fails
            workflowIndex.failNextWrite(new IllegalStateException("database down"));

            // then - the checkpoint writes its changes again
            assertThat(workflowIndex.pendingWrites()).singleElement().satisfies(
                    write -> assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of("order-1"), Set.of()))
            );
            workflowIndex.completeNextWrite();
            await(checkpoint);
        }

        @Test
        void failsWhenTheWriteFailsAndWritesTheChangesWithTheNextCheckpoint() {
            // given
            claim(segment);
            projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            workflowIndex.failWritesImmediately(new IllegalStateException("database down"));

            // when
            var failed = projection.flushForCheckpoint(segment, null);

            // then
            assertThat(failureOf(failed)).hasMessage("database down");

            // when - the index is available again
            workflowIndex.completeWritesImmediately();
            await(projection.flushForCheckpoint(segment, null));

            // then - no change was lost
            assertThat(stored(segment)).containsExactly("order-1");
        }

        @Test
        void failsWhenTheWriteDoesNotCompleteInTime() {
            // given
            claim(segment);
            projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            projection.checkpointTimeout = Duration.ofMillis(10);

            // when
            var checkpoint = projection.flushForCheckpoint(segment, null);

            // then
            assertThat(failureOf(checkpoint)).isInstanceOf(TimeoutException.class);
        }

        @Test
        void boundsTheTimeACheckpointWaitsByDefault() {
            // then
            assertThat(RunningWorkflowsProjection.DEFAULT_CHECKPOINT_TIMEOUT).isPositive();
        }

        @Test
        void writesTheStartOfAWorkflowWithALongId() {
            // given
            workflowIndex.completeWritesImmediately();
            claim(segment);

            // when
            projection.apply(segment, lifecycleEvent(LONG_ID, WorkflowStatus.STARTED));
            await(projection.flushForCheckpoint(segment, null));

            // then
            assertThat(stored(segment)).containsExactly(LONG_ID);
        }

        @Test
        void keepsTheChangesOfAWriteWithinAContextPendingWhenThatContextRollsBack() {
            // given
            workflowIndex.completeWritesImmediately();
            claim(segment);
            projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.STARTED));

            // when - the checkpoint writes within a context that fails after the write
            var checkpoint = unitOfWork();
            checkpoint.onPrepareCommit(context -> projection.flushForCheckpoint(segment, context));
            checkpoint.runOnCommit(context -> {
                throw new IllegalStateException("commit failed");
            });
            assertThat(checkpoint.execute()).failsWithin(TIMEOUT);

            // then - the next checkpoint writes the change again
            await(projection.flushForCheckpoint(segment, null));
            assertThat(workflowIndex.issuedWrites()).hasSize(2).allSatisfy(
                    write -> assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of("order-1"), Set.of()))
            );
        }

        @Test
        void keepsAChangeAppliedAfterAWriteWithinAContextThatRollsBack() {
            // given
            workflowIndex.completeWritesImmediately();
            claim(segment);
            projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            var checkpoint = unitOfWork();
            checkpoint.onPrepareCommit(context -> projection.flushForCheckpoint(segment, context));
            // a later change of the same workflow, applied before the context failed
            checkpoint.runOnCommit(context -> {
                projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.COMPLETED));
                throw new IllegalStateException("commit failed");
            });

            // when
            assertThat(checkpoint.execute()).failsWithin(TIMEOUT);
            await(projection.flushForCheckpoint(segment, null));

            // then - the later change wins
            assertThat(workflowIndex.issuedWrites()).last().satisfies(
                    write -> assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of(), Set.of("order-1")))
            );
        }

        @Test
        void doesNotRestoreAChangeAWriteOfALaterChangeReplacedWhenTheContextRollsBack() {
            // given
            workflowIndex.completeWritesImmediately();
            claim(segment);
            projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            var checkpoint = unitOfWork();
            checkpoint.onPrepareCommit(context -> projection.flushForCheckpoint(segment, context));
            // a later change of the same workflow, written before the context failed
            checkpoint.runOnCommit(context -> {
                projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.COMPLETED));
                await(projection.flushForCheckpoint(segment, null));
                throw new IllegalStateException("commit failed");
            });

            // when
            assertThat(checkpoint.execute()).failsWithin(TIMEOUT);
            await(projection.flushForCheckpoint(segment, null));

            // then - the stale start is not written again over the later removal
            assertThat(workflowIndex.issuedWrites()).hasSize(2).last().satisfies(
                    write -> assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of(), Set.of("order-1")))
            );
        }

        @Test
        void waitsForAWriteInFlightAndThenWritesInATransactionOfItsOwn() {
            // given - a write of an earlier checkpoint that is still in flight
            claim(segment);
            projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            projection.checkpointTimeout = Duration.ofMillis(10);
            failureOf(projection.flushForCheckpoint(segment, null));
            projection.checkpointTimeout = TIMEOUT;
            projection.apply(segment, lifecycleEvent("order-2", WorkflowStatus.STARTED));
            var checkpoint = unitOfWork();
            var flushed = new AtomicReference<CompletableFuture<Void>>();
            checkpoint.runOnPrepareCommit(context -> flushed.set(projection.flushForCheckpoint(segment, context)));
            await(checkpoint.execute());

            // when
            workflowIndex.completeNextWrite();

            // then - the follow-up runs on another thread, so it does not use the context
            assertThat(workflowIndex.pendingWrites()).singleElement().satisfies(
                    write -> assertThat(write.context()).isNull()
            );
            workflowIndex.completeNextWrite();
            await(flushed.get());
        }

        @Test
        void writesNothingAgainOnceTheContextOfAWriteCommitted() {
            // given
            workflowIndex.completeWritesImmediately();
            claim(segment);
            projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.STARTED));

            // when
            var checkpoint = unitOfWork();
            checkpoint.onPrepareCommit(context -> projection.flushForCheckpoint(segment, context));
            await(checkpoint.execute());
            await(projection.flushForCheckpoint(segment, null));

            // then
            assertThat(workflowIndex.issuedWrites()).hasSize(1);
        }

        @Test
        void failsForASegmentThatIsNotTracked() {
            // when
            var checkpoint = projection.flushForCheckpoint(anotherSegmentThan(FOUR_SEGMENTS.getFirst()), null);

            // then - its claim did not finish loading, so its handled events were never applied
            assertThat(failureOf(checkpoint)).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void failsOnceTheSegmentIsDiscarded() {
            // given
            claim(segment);
            projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.STARTED));

            // when
            projection.discard(segment);

            // then
            assertThat(failureOf(projection.flushForCheckpoint(segment, null))).isInstanceOf(IllegalStateException.class);
            assertThat(workflowIndex.issuedWrites()).isEmpty();
        }
    }

    @Nested
    class FlushForRelease {

        private final Segment segment = Segment.ROOT_SEGMENT;

        @BeforeEach
        void setUp() {
            workflowIndex.initialized(INDEX);
            claim(segment);
        }

        @Test
        void writesThePendingChanges() {
            // given
            projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            workflowIndex.completeWritesImmediately();

            // when
            await(projection.flushForRelease(segment));

            // then
            assertThat(stored(segment)).containsExactly("order-1");
        }

        @Test
        void writesTheChangesHandledWhileAWriteWasInFlightAfterIt() {
            // given - a write of an earlier checkpoint that is still in flight
            projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            projection.checkpointTimeout = Duration.ofMillis(10);
            failureOf(projection.flushForCheckpoint(segment, null));
            projection.apply(segment, lifecycleEvent("order-2", WorkflowStatus.STARTED));

            // when
            var released = projection.flushForRelease(segment);
            workflowIndex.completeNextWrite();
            workflowIndex.completeNextWrite();
            await(released);

            // then
            assertThat(stored(segment)).containsExactlyInAnyOrder("order-1", "order-2");
        }

        @Test
        void failsWhenTheWriteFails() {
            // given
            projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.STARTED));
            workflowIndex.failWritesImmediately(new IllegalStateException("database down"));

            // when
            var released = projection.flushForRelease(segment);

            // then - the release does not advance the stored token
            assertThat(failureOf(released)).hasMessage("database down");
        }

        @Test
        void givesUpWithinTheTimeTheProcessorGivesTheReleaseByDefault() {
            // then - the processor waits five seconds for the release by default
            assertThat(RunningWorkflowsProjection.DEFAULT_RELEASE_TIMEOUT).isLessThan(Duration.ofSeconds(5));
        }

        @Test
        void failsWhenTheWriteDoesNotSettleInTime() {
            // given
            projection.releaseTimeout = Duration.ofMillis(10);
            projection.apply(segment, lifecycleEvent("order-1", WorkflowStatus.STARTED));

            // when
            var released = projection.flushForRelease(segment);

            // then
            assertThat(failureOf(released)).isInstanceOf(TimeoutException.class);
            assertThat(workflowIndex.pendingWrites()).hasSize(1);
        }
    }


    @Nested
    class Discard {

        @Test
        void stopsTrackingASegmentThatWasSplitWithoutARelease() {
            // given - a claim of the root segment, split by the processor without notifying the engine
            claim(Segment.ROOT_SEGMENT);

            // when - one of its halves is claimed
            projection.discard(Segment.ROOT_SEGMENT.split()[1]);

            // then
            assertThat(projection.flushForCheckpoint(Segment.ROOT_SEGMENT, null)).isCompletedExceptionally();
        }

        @Test
        void stopsTrackingTheSegmentsMergedWithoutARelease() {
            // given
            var halves = Segment.ROOT_SEGMENT.split();
            claim(halves[0]);
            claim(halves[1]);

            // when - the merged segment is claimed
            projection.discard(Segment.ROOT_SEGMENT);

            // then
            assertThat(projection.flushForCheckpoint(halves[0], null)).isCompletedExceptionally();
            assertThat(projection.flushForCheckpoint(halves[1], null)).isCompletedExceptionally();
        }

        @Test
        void keepsTrackingTheSegmentsThatDoNotOverlap() {
            // given
            var halves = Segment.ROOT_SEGMENT.split();
            claim(halves[0]);
            claim(halves[1]);

            // when
            projection.discard(halves[0]);

            // then
            assertThat(projection.flushForCheckpoint(halves[0], null)).isCompletedExceptionally();
            assertThat(projection.flushForCheckpoint(halves[1], null)).isCompleted();
        }
    }

    @Nested
    class WithoutAnIndex {

        private final RunningWorkflowsProjection withoutIndex = new RunningWorkflowsProjection(
                WorkflowIndex.none(), INDEX
        );

        @Test
        void neverWaitsForTheIndex() {
            // given
            withoutIndex.initialize(Segment.ROOT_SEGMENT, new ClaimedIndex(Set.of(), Map.of("order-1", true)));

            // when
            withoutIndex.apply(Segment.ROOT_SEGMENT, lifecycleEvent(LONG_ID, WorkflowStatus.STARTED));

            // then - also for a segment that is not tracked
            assertThat(withoutIndex.flushForCheckpoint(Segment.ROOT_SEGMENT, null)).isCompleted();
            assertThat(withoutIndex.flushForCheckpoint(anotherSegmentThan(FOUR_SEGMENTS.getFirst()), null)).isCompleted();
            assertThat(withoutIndex.flushForRelease(Segment.ROOT_SEGMENT)).isCompleted();
        }
    }

    @Nested
    class StaleInstances {

        @Test
        void areWrittenAsRemovalsWithTheFirstCheckpointAfterTheClaim() {
            // given - the index still lists order-1, which already completed
            workflowIndex.initialized(INDEX, "order-1").completeWritesImmediately();
            var claimed = new ClaimedIndex(Set.of("order-1"), Map.of()).withoutStale(Set.of("order-1"));
            projection.initialize(Segment.ROOT_SEGMENT, claimed);

            // when
            await(projection.flushForCheckpoint(Segment.ROOT_SEGMENT, null));

            // then
            assertThat(stored(Segment.ROOT_SEGMENT)).isEmpty();
        }
    }

    @Nested
    class HibernationHints {

        private final Segment segment = Segment.ROOT_SEGMENT;
        private final HibernationHint hint = new HibernationHint(
                Instant.parse("2026-11-21T10:00:00Z"),
                List.of(new OpenWait("awaitPayment", "com.acme.PaymentReceived", Set.of("payload:orderId=42")))
        );

        @BeforeEach
        void setUp() {
            workflowIndex.initialized(INDEX, "order-1").completeWritesImmediately();
            claim(segment);
        }

        private Set<Entry> entries() {
            return workflowIndex.durableEntries(INDEX, segment);
        }

        private static IndexDelta hinted(String workflowId, HibernationHint hint) {
            var running = new HashMap<String, HibernationHint>();
            running.put(workflowId, hint);
            return new IndexDelta(running, Set.of());
        }

        /**
         * Takes a checkpoint whose write may join the transaction of its context, as the processor's checkpoints do.
         */
        private void checkpointWithinAContext() {
            var checkpoint = unitOfWork();
            checkpoint.onPrepareCommit(context -> projection.flushForCheckpoint(segment, context));
            await(checkpoint.execute());
        }

        @Test
        void writesAHintWithACheckpointWhoseWriteJoinsItsContext() {
            // when
            projection.recordHibernated(segment, "order-1", hint);
            checkpointWithinAContext();

            // then
            assertThat(workflowIndex.issuedWrites()).singleElement().satisfies(write -> {
                assertThat(write.context()).isNotNull();
                assertThat(write.delta()).isEqualTo(hinted("order-1", hint));
            });
            assertThat(entries()).containsExactly(new Entry("order-1", hint));
        }

        @Test
        void keepsAHintPendingWhenTheWriteCannotJoinTheContextAndWritesItOnceAWriteJoins() {
            // given
            workflowIndex.refuseToJoin(true);
            projection.recordHibernated(segment, "order-1", hint);
            projection.apply(segment, lifecycleEvent("order-2", WorkflowStatus.STARTED));

            // when - the index writes in a transaction of its own
            checkpointWithinAContext();

            // then - the workflow is written as running, without its hint
            assertThat(workflowIndex.issuedWrites()).singleElement().satisfies(write -> {
                assertThat(write.context()).isNull();
                assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of("order-1", "order-2"), Set.of()));
            });
            assertThat(entries()).containsExactlyInAnyOrder(Entry.running("order-1"), Entry.running("order-2"));

            // when - later checkpoints of their own have nothing to write
            checkpointWithinAContext();
            await(projection.flushForCheckpoint(segment, null));

            // then
            assertThat(workflowIndex.issuedWrites()).hasSize(1);

            // when - a later write joins
            workflowIndex.refuseToJoin(false);
            checkpointWithinAContext();

            // then - the pending hint is written
            assertThat(workflowIndex.issuedWrites()).hasSize(2).last().satisfies(
                    write -> assertThat(write.delta()).isEqualTo(hinted("order-1", hint))
            );
            assertThat(entries()).contains(new Entry("order-1", hint));
        }

        @Test
        void neverWritesAHintWithoutAContext() {
            // given
            projection.recordHibernated(segment, "order-1", hint);

            // when
            await(projection.flushForCheckpoint(segment, null));

            // then
            assertThat(workflowIndex.issuedWrites()).singleElement().satisfies(
                    write -> assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of("order-1"), Set.of()))
            );
            assertThat(entries()).containsExactly(Entry.running("order-1"));
        }

        @Test
        void releaseWritesTheWorkflowWithoutItsHint() {
            // given
            projection.recordHibernated(segment, "order-1", hint);

            // when
            await(projection.flushForRelease(segment));

            // then - a release writes in a transaction of the index's own, which could land late
            assertThat(workflowIndex.issuedWrites()).singleElement().satisfies(write -> {
                assertThat(write.context()).isNull();
                assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of("order-1"), Set.of()));
            });
            assertThat(entries()).containsExactly(Entry.running("order-1"));
        }

        @Test
        void keepsAHintPendingWhenTheContextOfItsWriteRollsBack() {
            // given
            projection.recordHibernated(segment, "order-1", hint);
            var failing = unitOfWork();
            failing.onPrepareCommit(context -> projection.flushForCheckpoint(segment, context));
            failing.runOnCommit(context -> {
                throw new IllegalStateException("commit failed");
            });
            assertThat(failing.execute()).failsWithin(TIMEOUT);

            // when
            checkpointWithinAContext();

            // then - the hint is written again
            assertThat(workflowIndex.issuedWrites()).hasSize(2).allSatisfy(
                    write -> assertThat(write.delta()).isEqualTo(hinted("order-1", hint))
            );
        }

        @Test
        void clearsTheHintOfAWorkflowThatWoke() {
            // given
            projection.recordHibernated(segment, "order-1", hint);
            checkpointWithinAContext();

            // when
            projection.recordWoken(segment, "order-1");
            await(projection.flushForCheckpoint(segment, null));

            // then
            assertThat(workflowIndex.issuedWrites()).last().satisfies(
                    write -> assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of("order-1"), Set.of()))
            );
            assertThat(entries()).containsExactly(Entry.running("order-1"));
        }

        @Test
        void aWakeBeforeTheHintWasWrittenReplacesThePendingHint() {
            // given
            projection.recordHibernated(segment, "order-1", hint);

            // when
            projection.recordWoken(segment, "order-1");
            checkpointWithinAContext();

            // then
            assertThat(workflowIndex.issuedWrites()).singleElement().satisfies(
                    write -> assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of("order-1"), Set.of()))
            );
        }

        @Test
        void writesNothingForAWakeOfAWorkflowWithoutAHint() {
            // when
            projection.recordWoken(segment, "order-1");
            checkpointWithinAContext();

            // then
            assertThat(workflowIndex.issuedWrites()).isEmpty();
        }

        @Test
        void anyEventOfAHintedWorkflowClearsItsHint() {
            // given
            projection.recordHibernated(segment, "order-1", hint);
            checkpointWithinAContext();

            // when - an event of the workflow that does not change whether it runs
            projection.apply(segment, stepEvent("order-1"));
            checkpointWithinAContext();

            // then
            assertThat(entries()).containsExactly(Entry.running("order-1"));
        }

        @Test
        void anEventPublishedByAHintedWorkflowKeepsItsHint() {
            // given
            projection.recordHibernated(segment, "order-1", hint);
            checkpointWithinAContext();

            // when - a business event the workflow published, which carries its id
            projection.apply(segment, publishedEvent("order-1", WorkflowStatus.STARTED));
            checkpointWithinAContext();

            // then
            assertThat(workflowIndex.issuedWrites()).hasSize(1);
            assertThat(entries()).containsExactly(new Entry("order-1", hint));
        }

        @Test
        void anEventOfAWorkflowWhoseHintTheClaimAdoptedClearsTheHint() {
            // given - a claim that left order-2 hibernated
            workflowIndex.stored(INDEX, new Entry("order-2", hint));
            projection.initialize(segment, new ClaimedIndex(Set.of(), Map.of("order-2", hint), Map.of()));

            // when
            projection.apply(segment, stepEvent("order-2"));
            checkpointWithinAContext();

            // then
            assertThat(workflowIndex.issuedWrites()).singleElement().satisfies(
                    write -> assertThat(write.delta()).isEqualTo(new IndexDelta(Set.of("order-2"), Set.of()))
            );
            assertThat(entries()).contains(Entry.running("order-2"));
        }

        @Test
        void removesAWorkflowThatTurnedOutTerminalOnWake() {
            // when
            projection.recordRemoved(segment, "order-1");
            await(projection.flushForCheckpoint(segment, null));

            // then
            assertThat(entries()).isEmpty();
        }

        @Test
        void ignoresHintsOfASegmentThatIsNotTracked() {
            // given
            var untracked = FOUR_SEGMENTS.getFirst();

            // when
            projection.recordHibernated(untracked, anyIdOn(untracked), hint);
            checkpointWithinAContext();

            // then
            assertThat(workflowIndex.issuedWrites()).isEmpty();
        }
    }
}
