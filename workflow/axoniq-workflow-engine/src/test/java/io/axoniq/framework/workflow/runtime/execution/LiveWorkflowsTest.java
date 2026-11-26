/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.OpenWait;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution.CheckpointWorkStateListener;
import io.axoniq.framework.workflow.runtime.execution.LiveWorkflows.Hibernated;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.IntStream;

import static io.axoniq.framework.workflow.runtime.execution.SegmentTestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Tests the transitions of {@link LiveWorkflows} that bring a workflow into memory, take it out again, and move it
 * between memory and hibernation, the safe points they track, which only the execution in memory may report, how an
 * event finds the live workflows, also those that hibernate or wake while it looks them up, and how a cancellation
 * request for a woken execution ends when that execution takes no task.
 * <p>
 * The hibernated workflows wake through a real {@link WorkflowEngineHibernationSupport}, whose restorer is replaced by
 * one that records the wake and restores a stand-in execution, which the support swaps into memory in place of the
 * hibernation.
 *
 * @author Allard Buijze
 */
class LiveWorkflowsTest {

    private static final Segment SEGMENT = FOUR_SEGMENTS.getFirst();
    private static final String WORKFLOW_ID = anyIdOn(SEGMENT);
    private static final String OTHER_ID = IntStream.range(0, 512)
                                                    .mapToObj(i -> "sharded-" + i)
                                                    .filter(id -> !id.equals(WORKFLOW_ID))
                                                    .filter(id -> WorkflowSegmentOwnership.ownedBy(SEGMENT, id))
                                                    .findFirst()
                                                    .orElseThrow();
    private static final String FOREIGN_ID = idOnAnotherSegmentThan(WORKFLOW_ID);

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final HookRepository repository = new HookRepository();
    private final LiveWorkflows liveWorkflows = new LiveWorkflows(repository);
    private final SegmentClaims segmentClaims = new SegmentClaims();
    private final SegmentClaim claim = startClaim(segmentClaims);
    private final List<String> loads = new CopyOnWriteArrayList<>();
    private final WorkflowEngineHibernationSupport support = new WorkflowEngineHibernationSupport(
            new ControllableWorkflowScheduler(),
            Clock.systemUTC(),
            liveWorkflows,
            new RunningWorkflowsProjection(WorkflowIndex.none(), "orders"),
            // Records the wake, and restores a stand-in execution, which reports that it runs, so no body starts.
            (workflowId, resources, executionContext) -> {
                loads.add(workflowId);
                return CompletableFuture.completedFuture(
                        Optional.of(new InspectionCountingWorkflowExecution(workflowId))
                );
            },
            new WorkflowStarter(liveWorkflows),
            Duration.ofMinutes(1),
            Duration.ofSeconds(10),
            Runnable::run
    );

    private static SegmentClaim startClaim(SegmentClaims segmentClaims) {
        segmentClaims.initialize(null);
        return segmentClaims.startClaim(SEGMENT, new StubProcessingContext());
    }

    private static EventMessage paid(String orderId) {
        return new GenericEventMessage(new MessageType("paid"), Map.of("orderId", orderId));
    }

    /**
     * Returns a hibernation of the given workflow under the claim of the segment, waiting for the payment of the given
     * order.
     */
    private Hibernated awaitingPayment(String workflowId, String orderId) {
        var hint = new HibernationHint(Instant.now().plus(Duration.ofHours(1)), List.of(
                new OpenWait("awaitPaid", "paid", Set.of("payload:orderId=" + orderId))
        ));
        return support.hibernationOf(claim, workflowId, hint, CompletableFuture.completedFuture(null));
    }

    /**
     * Adopts the given hibernation, under the locks the hibernation support takes.
     */
    private void adopt(Hibernated hibernated) {
        try (var ignored = liveWorkflows.lock(hibernated.workflowId())) {
            assertThat(claim.runIfCurrent(() -> assertThat(liveWorkflows.adopt(hibernated)).isTrue())).isTrue();
        }
    }

    /**
     * Swaps the given execution for the given hibernation, under the locks the hibernation support takes.
     */
    private boolean hibernate(WorkflowExecution execution, Hibernated hibernated) {
        var hibernatedNow = new AtomicBoolean();
        try (var ignored = liveWorkflows.lock(hibernated.workflowId())) {
            claim.runIfCurrent(() -> hibernatedNow.set(liveWorkflows.hibernate(execution, hibernated)));
        }
        return hibernatedNow.get();
    }

    /**
     * Swaps the given hibernation for the given execution, under the locks the engine takes.
     */
    private boolean wake(Hibernated hibernated, WorkflowExecution execution) {
        var woken = new AtomicBoolean();
        try (var ignored = liveWorkflows.lock(hibernated.workflowId())) {
            claim.runIfCurrent(() -> woken.set(liveWorkflows.wake(hibernated, execution, Runnable::run)));
        }
        return woken.get();
    }

    /**
     * Delivers the given event to the given workflow, as the engine does: a hibernated workflow wakes through the
     * hibernation support.
     */
    private boolean deliver(String workflowId, EventMessage event) {
        return liveWorkflows.deliver(workflowId, event, new StubProcessingContext(), support::wakeForEvent);
    }

    /**
     * Delivers the given broadcast event to the workflows of the given segment, as the engine does: a hibernated
     * workflow that waits for it wakes through the hibernation support.
     */
    private void broadcast(Segment segment, EventMessage event) {
        liveWorkflows.broadcast(segment, event, new StubProcessingContext(), support::wakeForEvent);
    }

    /**
     * Runs the given action on another thread, and returns whether it completed in time.
     */
    private static boolean completesOnAnotherThread(Runnable action) {
        var done = new CountDownLatch(1);
        Thread.ofPlatform().start(() -> {
            action.run();
            done.countDown();
        });
        try {
            return done.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Returns whether the given thread waits for the swap lock of the registry, which its {@link BroadcastAddressees}
     * keeps: the only {@link StampedLock} it uses.
     */
    private static boolean isParkedOnTheSwapLock(Thread thread) {
        return LockSupport.getBlocker(thread) instanceof StampedLock;
    }

    /**
     * Returns whether the given thread waits, for a lock that tells no blocker object, like the lock of a workflow.
     */
    private static boolean isBlocked(Thread thread) {
        var state = thread.getState();
        return state == Thread.State.BLOCKED || state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
    }

    private static CheckpointWorkStateListener safePointsOf(InspectionCountingWorkflowExecution execution) {
        return Objects.requireNonNull(execution.checkpointWorkStateListener(),
                                      "The execution got no checkpoint work state listener.");
    }

    @Nested
    class Entering {

        @Test
        void bringsTheCreatedExecutionIntoMemoryAndTheRepository() {
            // given
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);

            // when
            WorkflowExecution entered = liveWorkflows.enter(WORKFLOW_ID, false, () -> execution);

            // then
            assertThat(entered).isSameAs(execution);
            assertThat(liveWorkflows.isInMemory(execution)).isTrue();
            assertThat(liveWorkflows.find(WORKFLOW_ID)).containsSame(execution);
            assertThat(repository.findById(WORKFLOW_ID)).containsSame(execution);
            assertThat(liveWorkflows.listInMemory()).containsExactly(execution);
            assertThat(execution.checkpointWorkStateListener())
                    .as("an execution in memory reports its safe points")
                    .isNotNull();
        }

        @Test
        void returnsTheExecutionInMemoryWithoutCreatingAnother() {
            // given
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> execution);

            // when
            WorkflowExecution entered = liveWorkflows.enter(WORKFLOW_ID, false, () -> {
                throw new AssertionError("The factory must not run for a workflow in memory.");
            });

            // then
            assertThat(entered).isSameAs(execution);
            assertThat(repository.findById(WORKFLOW_ID)).containsSame(execution);
        }

        @Test
        void failsAndLeavesNothingInMemoryWhenTheRepositoryHoldsAnotherExecutionOfTheWorkflow() {
            // given - an execution stored in the repository without having entered memory through the registry
            var stored = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            repository.save(WORKFLOW_ID, () -> stored);
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);

            // when / then
            assertThatThrownBy(() -> liveWorkflows.enter(WORKFLOW_ID, false, () -> execution))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(WORKFLOW_ID);
            assertThat(liveWorkflows.find(WORKFLOW_ID)).isEmpty();
            assertThat(liveWorkflows.isInMemory(execution)).isFalse();
            assertThat(repository.findById(WORKFLOW_ID)).containsSame(stored);
            assertThat(execution.checkpointWorkStateListener())
                    .as("an execution that did not enter memory reports no safe points")
                    .isNull();
        }

        @Test
        void entersAgainOnceTheConflictingExecutionIsGoneFromTheRepository() {
            // given - a failed enter, whose entry must not linger
            var stored = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            repository.save(WORKFLOW_ID, () -> stored);
            var failed = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            assertThatThrownBy(() -> liveWorkflows.enter(WORKFLOW_ID, false, () -> failed))
                    .isInstanceOf(IllegalStateException.class);
            repository.remove(WORKFLOW_ID, stored);
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);

            // when
            WorkflowExecution entered = liveWorkflows.enter(WORKFLOW_ID, false, () -> execution);

            // then
            assertThat(entered).isSameAs(execution);
            assertThat(repository.findById(WORKFLOW_ID)).containsSame(execution);
        }
    }

    @Nested
    class Leaving {

        @Test
        void takesTheExecutionOutOfMemoryAndTheRepository() {
            // given
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> execution);

            // when
            boolean left = liveWorkflows.leave(execution);

            // then
            assertThat(left).isTrue();
            assertThat(liveWorkflows.isInMemory(execution)).isFalse();
            assertThat(liveWorkflows.find(WORKFLOW_ID)).isEmpty();
            assertThat(repository.findById(WORKFLOW_ID)).isEmpty();
        }

        @Test
        void doesNothingForAnExecutionThatNeverEntered() {
            // given
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);

            // when
            boolean left = liveWorkflows.leave(execution);

            // then
            assertThat(left).isFalse();
        }

        @Test
        void neverTakesTheSuccessorOfAnExecutionThatLeftOutOfMemory() {
            // given - an execution that left memory, and the next execution of its workflow
            var departed = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> departed);
            liveWorkflows.leave(departed);
            var successor = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> successor);

            // when - the execution that left leaves again, as a stop racing its own end does
            boolean left = liveWorkflows.leave(departed);

            // then
            assertThat(left).isFalse();
            assertThat(liveWorkflows.find(WORKFLOW_ID)).containsSame(successor);
            assertThat(repository.findById(WORKFLOW_ID)).containsSame(successor);
        }

        @Test
        void removesAnExecutionFromTheRepositoryAlsoWhenItHasNoEntry() {
            // given - an execution stored in the repository without having entered memory through the registry
            var stray = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            repository.save(WORKFLOW_ID, () -> stray);

            // when
            boolean left = liveWorkflows.leave(stray);

            // then - it was not in memory, but it is no longer stuck in the repository either
            assertThat(left).isFalse();
            assertThat(repository.findById(WORKFLOW_ID)).isEmpty();
        }

        @Test
        void anEnterWhileTheExecutionLeavesWaitsForItAndNeverGetsTheLeavingExecutionBack() throws Exception {
            // given - an execution whose leave is held while it removes the execution from the repository
            var departing = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> departing);
            var removing = new CountDownLatch(1);
            var mayRemove = new CountDownLatch(1);
            repository.beforeNextRemoval(() -> {
                removing.countDown();
                awaitUninterruptibly(mayRemove);
            });
            var leave = new CompletableFuture<Boolean>();
            Thread.ofPlatform().start(() -> leave.complete(liveWorkflows.leave(departing)));
            assertThat(removing.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isTrue();

            // when - the workflow enters memory again while its execution is still leaving
            var successor = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            var entered = new CompletableFuture<WorkflowExecution>();
            var entering = Thread.ofPlatform().start(
                    () -> entered.complete(liveWorkflows.enter(WORKFLOW_ID, false, () -> successor))
            );
            await().atMost(TIMEOUT).until(() -> entering.getState() == Thread.State.BLOCKED || entered.isDone());

            // then - the enter waits for the leave, after which the successor enters
            assertThat(entered).as("an enter must wait for a leave of the workflow in progress").isNotDone();
            mayRemove.countDown();
            assertThat(leave.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isTrue();
            assertThat(entered.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isSameAs(successor);
            assertThat(liveWorkflows.find(WORKFLOW_ID)).containsSame(successor);
            assertThat(repository.findById(WORKFLOW_ID)).containsSame(successor);
        }

        @Test
        void clearingTakesEveryExecutionOutOfMemoryAndForgetsItsSafePoint() {
            // given
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> execution);
            safePointsOf(execution).onMarkedUnsafe();

            // when
            liveWorkflows.clear();

            // then
            assertThat(liveWorkflows.find(WORKFLOW_ID)).isEmpty();
            assertThat(repository.findAll()).isEmpty();
            assertThat(liveWorkflows.safePoints().unsafeOf(SEGMENT)).isEmpty();
        }
    }

    @Nested
    class SafePoints {

        @Test
        void listsTheUnsafeExecutionsOfTheGivenSegmentOnly() {
            // given
            var owned = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            var foreign = new InspectionCountingWorkflowExecution(FOREIGN_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> owned);
            liveWorkflows.enter(FOREIGN_ID, false, () -> foreign);

            // when
            safePointsOf(owned).onMarkedUnsafe();
            safePointsOf(foreign).onMarkedUnsafe();

            // then
            assertThat(liveWorkflows.safePoints().unsafeOf(SEGMENT))
                    .extracting(LiveWorkflows.InMemory::execution)
                    .containsExactly(owned);
        }

        @Test
        void forgetsTheUnsafeMarkOfAnExecutionThatMarksItselfSafe() {
            // given
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> execution);
            safePointsOf(execution).onMarkedUnsafe();

            // when
            safePointsOf(execution).onMarkedSafe();

            // then
            assertThat(liveWorkflows.safePoints().unsafeOf(SEGMENT)).isEmpty();
        }

        @Test
        void forgetsTheUnsafeMarkOfAnExecutionThatLeavesMemory() {
            // given
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> execution);
            safePointsOf(execution).onMarkedUnsafe();

            // when
            liveWorkflows.leave(execution);

            // then
            assertThat(liveWorkflows.safePoints().unsafeOf(SEGMENT)).isEmpty();
        }

        @Test
        void forgetsAMarkUnsafeThatArrivesWhileTheExecutionLeavesMemory() {
            // given - an execution that reports unsafe work while its leave removes it from the repository
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> execution);
            var unsafeDuringLeave = new AtomicReference<Integer>();
            repository.beforeNextRemoval(() -> {
                safePointsOf(execution).onMarkedUnsafe();
                unsafeDuringLeave.set(liveWorkflows.safePoints().unsafeOf(SEGMENT).size());
            });

            // when
            liveWorkflows.leave(execution);

            // then - the mark was taken while the execution was still in memory, and its leave removed it
            assertThat(unsafeDuringLeave).hasValue(1);
            assertThat(liveWorkflows.safePoints().unsafeOf(SEGMENT)).isEmpty();
        }

        @Test
        void ignoresALateMarkUnsafeOfAnExecutionThatLeftMemory() {
            // given - an execution that left memory, and its successor, which has no unsafe work
            var departed = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> departed);
            liveWorkflows.leave(departed);
            var successor = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> successor);
            safePointsOf(successor).onMarkedSafe();

            // when - the execution that left keeps unwinding, and reports unsafe work
            safePointsOf(departed).onMarkedUnsafe();

            // then
            assertThat(liveWorkflows.safePoints().unsafeOf(SEGMENT))
                    .as("an execution that left memory must not mark its successor's workflow unsafe")
                    .isEmpty();
        }

        @Test
        void keepsTheUnsafeMarkOfTheSuccessorWhenTheExecutionThatLeftMemoryReportsLate() {
            // given - an execution that left memory while unsafe, and its successor, which is unsafe as well
            var departed = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> departed);
            safePointsOf(departed).onMarkedUnsafe();
            liveWorkflows.leave(departed);
            var successor = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> successor);
            safePointsOf(successor).onMarkedUnsafe();

            // when - the execution that left keeps unwinding, and reports both unsafe and safe work
            safePointsOf(departed).onMarkedUnsafe();
            safePointsOf(departed).onMarkedSafe();

            // then
            assertThat(liveWorkflows.safePoints().unsafeOf(SEGMENT))
                    .as("an execution that left memory must not replace or clear its successor's unsafe mark")
                    .extracting(LiveWorkflows.InMemory::execution)
                    .containsExactly(successor);
        }

        @Test
        void markingAnEntrySafeLeavesTheUnsafeMarkOfTheEntryThatReplacedIt() {
            // given - the entry of an unsafe execution, read before it left memory, and its unsafe successor
            var departed = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> departed);
            safePointsOf(departed).onMarkedUnsafe();
            var departedEntry = liveWorkflows.safePoints().unsafeOf(SEGMENT).getFirst();
            liveWorkflows.leave(departed);
            var successor = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> successor);
            safePointsOf(successor).onMarkedUnsafe();

            // when - a checkpoint that read the entry before it left finds its execution safe
            liveWorkflows.safePoints().markSafe(departedEntry);

            // then
            assertThat(liveWorkflows.safePoints().unsafeOf(SEGMENT))
                    .extracting(LiveWorkflows.InMemory::execution)
                    .containsExactly(successor);
        }
    }

    @Nested
    class Hibernation {

        @Test
        void hibernatingSwapsTheExecutionInMemoryForTheHibernation() {
            // given
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, true, () -> execution);
            safePointsOf(execution).onMarkedUnsafe();
            var hibernated = awaitingPayment(WORKFLOW_ID, "1");

            // when
            boolean hibernatedNow = hibernate(execution, hibernated);

            // then
            assertThat(hibernatedNow).isTrue();
            assertThat(liveWorkflows.get(WORKFLOW_ID)).isSameAs(hibernated);
            assertThat(liveWorkflows.find(WORKFLOW_ID)).isEmpty();
            assertThat(repository.findById(WORKFLOW_ID)).isEmpty();
            assertThat(liveWorkflows.hibernatedWorkflowIds()).containsExactly(WORKFLOW_ID);
            assertThat(liveWorkflows.safePoints().unsafeOf(SEGMENT)).isEmpty();
            assertThat(liveWorkflows.leave(execution))
                    .as("an execution that hibernated never takes its hibernation out of memory")
                    .isFalse();
            assertThat(liveWorkflows.get(WORKFLOW_ID)).isSameAs(hibernated);
        }

        @Test
        void doesNotHibernateAnExecutionThatLeftMemory() {
            // given
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, true, () -> execution);
            liveWorkflows.leave(execution);

            // when
            boolean hibernatedNow = hibernate(execution, awaitingPayment(WORKFLOW_ID, "1"));

            // then - and it left no waits behind that an event could wake
            assertThat(hibernatedNow).isFalse();
            assertThat(liveWorkflows.get(WORKFLOW_ID)).isNull();
            broadcast(SEGMENT, paid("1"));
            assertThat(loads).isEmpty();
        }

        @Test
        void wakingSwapsTheHibernationForTheExecutionAndForgetsItsWaits() {
            // given
            var hibernated = awaitingPayment(WORKFLOW_ID, "1");
            adopt(hibernated);
            var woken = new InspectionCountingWorkflowExecution(WORKFLOW_ID);

            // when
            boolean wokenNow = wake(hibernated, woken);

            // then
            assertThat(wokenNow).isTrue();
            assertThat(liveWorkflows.find(WORKFLOW_ID)).containsSame(woken);
            assertThat(repository.findById(WORKFLOW_ID)).containsSame(woken);
            assertThat(liveWorkflows.hibernatedWorkflowIds()).isEmpty();
            assertThat(woken.checkpointWorkStateListener())
                    .as("a woken execution reports its safe points")
                    .isNotNull();
            // and an awaited event reaches it in memory, without waking it again
            var event = paid("1");
            broadcast(SEGMENT, event);
            assertThat(woken.deliveredEvents()).containsExactly(event);
            assertThat(loads).isEmpty();
        }

        @Test
        void everyVisitOfTheEntriesFindsAWorkflowThatKeepsHibernatingAndWakingExactlyOnce()
                throws InterruptedException {
            // given - a workflow that hibernates and wakes on another thread, over and over, next to one that stays
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, true, () -> execution);
            liveWorkflows.enter(OTHER_ID, false, () -> new InspectionCountingWorkflowExecution(OTHER_ID));
            var stop = new AtomicBoolean();
            var swaps = new AtomicInteger();
            var swapper = Thread.ofPlatform().start(() -> {
                WorkflowExecution inMemory = execution;
                while (!stop.get()) {
                    var hibernated = awaitingPayment(WORKFLOW_ID, "1");
                    assertThat(hibernate(inMemory, hibernated)).isTrue();
                    inMemory = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
                    assertThat(wake(hibernated, inMemory)).isTrue();
                    swaps.incrementAndGet();
                }
            });

            // when - the entries are visited while it swaps
            var found = new ArrayList<Long>();
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            try {
                while (swaps.get() < 500 && System.nanoTime() < deadline) {
                    found.add(liveWorkflows.entries().filter(entry -> entry.workflowId().equals(WORKFLOW_ID)).count());
                }
            } finally {
                stop.set(true);
                swapper.join(TIMEOUT);
            }

            // then
            assertThat(swaps).hasValueGreaterThanOrEqualTo(500);
            assertThat(found).isNotEmpty().containsOnly(1L);
        }

        @Test
        void aRestoreTakesOverTheHibernationInPlaceAndForgetsItsWaits() {
            // given - a hibernation under another claim than the one that restored the workflow
            var hibernated = awaitingPayment(WORKFLOW_ID, "1");
            adopt(hibernated);
            var restored = new InspectionCountingWorkflowExecution(WORKFLOW_ID);

            // when
            boolean restoredNow;
            try (var ignored = liveWorkflows.lock(WORKFLOW_ID)) {
                restoredNow = liveWorkflows.restoreOver(hibernated, restored, false);
            }

            // then
            assertThat(restoredNow).isTrue();
            assertThat(liveWorkflows.find(WORKFLOW_ID)).containsSame(restored);
            assertThat(repository.findById(WORKFLOW_ID)).containsSame(restored);
            assertThat(liveWorkflows.hibernatedWorkflowIds()).isEmpty();
            assertThat(restored.checkpointWorkStateListener())
                    .as("a restored execution reports its safe points")
                    .isNotNull();
            // and an awaited event reaches it in memory, without waking it
            var event = paid("1");
            broadcast(SEGMENT, event);
            assertThat(restored.deliveredEvents()).containsExactly(event);
            assertThat(loads).isEmpty();
        }

        @Test
        void failsAWakeWhenTheRepositoryHoldsAnotherExecutionOfTheWorkflow() {
            // given
            var hibernated = awaitingPayment(WORKFLOW_ID, "1");
            adopt(hibernated);
            var stored = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            repository.save(WORKFLOW_ID, () -> stored);

            // when / then
            assertThatThrownBy(() -> wake(hibernated, new InspectionCountingWorkflowExecution(WORKFLOW_ID)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(WORKFLOW_ID);
            assertThat(liveWorkflows.get(WORKFLOW_ID)).isSameAs(hibernated);
            assertThat(repository.findById(WORKFLOW_ID)).containsSame(stored);
        }

        @Test
        void aWokenExecutionThatFailedToStartSwapsBackForTheSameHibernation() {
            // given
            var hibernated = awaitingPayment(WORKFLOW_ID, "1");
            adopt(hibernated);
            var woken = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            wake(hibernated, woken);

            // when
            boolean swappedBack = hibernate(woken, hibernated);

            // then - with its waits, which an awaited event wakes again
            assertThat(swappedBack).isTrue();
            assertThat(liveWorkflows.get(WORKFLOW_ID)).isSameAs(hibernated);
            assertThat(repository.findById(WORKFLOW_ID)).isEmpty();
            broadcast(SEGMENT, paid("1"));
            assertThat(loads).containsExactly(WORKFLOW_ID);
        }

        @Test
        void droppingForgetsTheHibernationAndItsWaits() {
            // given
            var hibernated = awaitingPayment(WORKFLOW_ID, "1");
            adopt(hibernated);

            // when
            boolean dropped;
            try (var ignored = liveWorkflows.lock(WORKFLOW_ID)) {
                dropped = liveWorkflows.drop(hibernated);
            }

            // then
            assertThat(dropped).isTrue();
            assertThat(liveWorkflows.get(WORKFLOW_ID)).isNull();
            assertThat(liveWorkflows.hibernatedWorkflowIds()).isEmpty();
            broadcast(SEGMENT, paid("1"));
            assertThat(loads).isEmpty();
        }

        @Test
        void droppingAHibernationThatWasReplacedLeavesItsReplacement() {
            // given - a hibernation whose timer was replaced, as a failed wake is retried
            var hibernated = awaitingPayment(WORKFLOW_ID, "1");
            adopt(hibernated);
            var replacement = hibernated.withWakeTimer(new ControllableWorkflowScheduler().schedule(Instant.now()));
            try (var ignored = liveWorkflows.lock(WORKFLOW_ID)) {
                assertThat(liveWorkflows.replace(hibernated, replacement)).isTrue();
            }

            // when
            boolean dropped;
            try (var ignored = liveWorkflows.lock(WORKFLOW_ID)) {
                dropped = liveWorkflows.drop(hibernated);
            }

            // then
            assertThat(dropped).isFalse();
            assertThat(liveWorkflows.get(WORKFLOW_ID)).isSameAs(replacement);
        }

        @Test
        void doesNotEnterAHibernatedWorkflow() {
            // given
            var hibernated = awaitingPayment(WORKFLOW_ID, "1");
            adopt(hibernated);

            // when / then
            assertThatThrownBy(() -> liveWorkflows.enter(
                    WORKFLOW_ID, true, () -> new InspectionCountingWorkflowExecution(WORKFLOW_ID)
            )).isInstanceOf(IllegalStateException.class).hasMessageContaining(WORKFLOW_ID);
            assertThat(liveWorkflows.get(WORKFLOW_ID)).isSameAs(hibernated);
            assertThat(repository.findById(WORKFLOW_ID)).isEmpty();
        }
    }

    @Nested
    class Broadcast {

        @Test
        void wakesOnlyTheHibernatedWorkflowsWhoseWaitsMayMatchTheEvent() {
            // given
            adopt(awaitingPayment(WORKFLOW_ID, "1"));
            adopt(awaitingPayment(OTHER_ID, "2"));

            // when
            broadcast(SEGMENT, paid("1"));

            // then - only the workflow awaiting it, on the handling thread
            assertThat(loads).containsExactly(WORKFLOW_ID);
            assertThat(liveWorkflows.hibernatedWorkflowIds()).containsExactly(OTHER_ID);
        }

        @Test
        void wakesNoHibernatedWorkflowOfAnotherSegment() {
            // given
            adopt(awaitingPayment(WORKFLOW_ID, "1"));

            // when
            broadcast(anotherSegmentThan(SEGMENT), paid("1"));

            // then
            assertThat(loads).isEmpty();
        }

        @Test
        void reachesAWorkflowThatWakesWhileTheExecutionsInMemoryAreListed() {
            // given - a hibernated workflow whose waits do not match the event, which wakes for another reason right
            // after the broadcast listed the executions in memory
            var hibernated = awaitingPayment(WORKFLOW_ID, "other");
            adopt(hibernated);
            var woken = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            repository.aroundNextListing(() -> {
            }, () -> assertThat(completesOnAnotherThread(() -> wake(hibernated, woken))).isTrue());
            var event = paid("1");

            // when
            broadcast(SEGMENT, event);

            // then - the lookup ran again, without a swap interleaving, and found it in memory
            assertThat(woken.deliveredEvents()).containsExactly(event);
            assertThat(repository.listings()).isEqualTo(2);
        }

        @Test
        void reachesAWorkflowThatHibernatesAndWakesWhileTheExecutionsInMemoryAreListed() {
            // given - a workflow in memory, which hibernates with waits that do not match the event right before the
            // broadcast lists the executions in memory, and wakes for another reason right after
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, true, () -> execution);
            var hibernated = awaitingPayment(WORKFLOW_ID, "other");
            var woken = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            repository.aroundNextListing(
                    () -> assertThat(completesOnAnotherThread(() -> hibernate(execution, hibernated))).isTrue(),
                    () -> assertThat(completesOnAnotherThread(() -> wake(hibernated, woken))).isTrue()
            );
            var event = paid("1");

            // when
            broadcast(SEGMENT, event);

            // then
            assertThat(woken.deliveredEvents()).containsExactly(event);
            assertThat(execution.deliveredEvents()).isEmpty();
        }

        @Test
        void holdsOffEverySwapWhileItLooksUpAgain() throws Exception {
            // given - a hibernated workflow whose waits do not match the event, which wakes on another thread as the
            // broadcast first lists the executions in memory, so the broadcast looks up again, and which tries to
            // hibernate again on another thread as the broadcast lists the executions in memory once more
            var hibernated = awaitingPayment(WORKFLOW_ID, "other");
            adopt(hibernated);
            var woken = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            var hibernatedAgain = awaitingPayment(WORKFLOW_ID, "other");
            var swappedAgain = new CountDownLatch(1);
            var swappedDuringTheSecondLookup = new AtomicBoolean();
            repository.beforeListing(1, () -> assertThat(
                    completesOnAnotherThread(() -> wake(hibernated, woken))
            ).isTrue());
            repository.beforeListing(2, () -> {
                var swapping = Thread.ofPlatform().start(() -> {
                    hibernate(woken, hibernatedAgain);
                    swappedAgain.countDown();
                });
                await().atMost(TIMEOUT).until(() -> swappedAgain.getCount() == 0 || isParkedOnTheSwapLock(swapping));
                swappedDuringTheSecondLookup.set(swappedAgain.getCount() == 0);
            });

            // when
            broadcast(SEGMENT, paid("1"));

            // then - the swap ran only once the second lookup ended
            assertThat(repository.listings()).isEqualTo(2);
            assertThat(swappedDuringTheSecondLookup).as("a swap interleaved the second lookup").isFalse();
            assertThat(swappedAgain.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isTrue();
            assertThat(liveWorkflows.get(WORKFLOW_ID)).isSameAs(hibernatedAgain);
        }

        @Test
        void waitsForASwapInProgressAndThenLooksUpWithoutHoldingOffTheNextSwap() throws Exception {
            // given - a workflow whose hibernation is in progress, held while it removes the execution from the
            // repository, and another hibernated workflow, whose waits do not match the event either
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, true, () -> execution);
            var hibernated = awaitingPayment(WORKFLOW_ID, "other");
            var other = awaitingPayment(OTHER_ID, "other");
            adopt(other);
            var removing = new CountDownLatch(1);
            var mayRemove = new CountDownLatch(1);
            repository.beforeNextRemoval(() -> {
                removing.countDown();
                awaitUninterruptibly(mayRemove);
            });
            Thread.ofPlatform().start(() -> hibernate(execution, hibernated));
            assertThat(removing.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isTrue();
            // the other workflow wakes on another thread as the broadcast first lists the executions in memory, which
            // it can only while that lookup holds off no swap
            var otherWoken = new InspectionCountingWorkflowExecution(OTHER_ID);
            var swappedDuringTheFirstLookup = new AtomicBoolean();
            repository.aroundNextListing(
                    () -> swappedDuringTheFirstLookup.set(completesOnAnotherThread(() -> wake(other, otherWoken))),
                    () -> {
                    }
            );
            var event = paid("1");

            // when
            var broadcast = new CompletableFuture<Void>();
            var broadcasting = Thread.ofPlatform().start(() -> {
                try {
                    broadcast(SEGMENT, event);
                    broadcast.complete(null);
                } catch (Throwable e) {
                    broadcast.completeExceptionally(e);
                }
            });
            await().atMost(TIMEOUT).until(() -> isParkedOnTheSwapLock(broadcasting) || broadcast.isDone());

            // then - it waits for the swap in progress before it lists anything
            assertThat(broadcast).isNotDone();
            assertThat(repository.listings()).isZero();
            mayRemove.countDown();
            broadcast.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            assertThat(swappedDuringTheFirstLookup).isTrue();
            assertThat(otherWoken.deliveredEvents()).containsExactly(event);
            assertThat(liveWorkflows.get(WORKFLOW_ID)).isSameAs(hibernated);
        }
    }

    @Nested
    class Delivery {

        @Test
        void deliversAnEventToAnExecutionThatMayHibernateOnlyUnderTheLockOfItsWorkflow() throws Exception {
            // given - another thread holds the lock of the workflow, as a hibernation in progress does
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, true, () -> execution);
            var locked = new CountDownLatch(1);
            var unlock = new CountDownLatch(1);
            Thread.ofPlatform().start(() -> {
                try (var ignored = liveWorkflows.lock(WORKFLOW_ID)) {
                    locked.countDown();
                    awaitUninterruptibly(unlock);
                }
            });
            assertThat(locked.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isTrue();
            var event = paid("1");

            // when
            var delivered = new CompletableFuture<Boolean>();
            var delivering = Thread.ofPlatform().start(
                    () -> delivered.complete(deliver(WORKFLOW_ID, event))
            );
            await().atMost(TIMEOUT).until(() -> isBlocked(delivering) || delivered.isDone());

            // then
            assertThat(execution.deliveredEvents()).as("delivered while the workflow was locked").isEmpty();
            unlock.countDown();
            assertThat(delivered.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isTrue();
            assertThat(execution.deliveredEvents()).containsExactly(event);
        }

        @Test
        void deliversAnEventToAnExecutionThatNeverHibernatesWithoutTheLockOfItsWorkflow() throws Exception {
            // given
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> execution);
            var locked = new CountDownLatch(1);
            var unlock = new CountDownLatch(1);
            Thread.ofPlatform().start(() -> {
                try (var ignored = liveWorkflows.lock(WORKFLOW_ID)) {
                    locked.countDown();
                    awaitUninterruptibly(unlock);
                }
            });
            assertThat(locked.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isTrue();
            var event = paid("1");

            // when
            boolean delivered;
            try {
                delivered = deliver(WORKFLOW_ID, event);
            } finally {
                unlock.countDown();
            }

            // then
            assertThat(delivered).isTrue();
            assertThat(execution.deliveredEvents()).containsExactly(event);
        }

        @Test
        void wakesAHibernatedWorkflowForAnEventOfItsOwnWhateverItWaitsFor() {
            // given
            adopt(awaitingPayment(WORKFLOW_ID, "other"));
            var event = paid("1");

            // when
            boolean delivered = deliver(WORKFLOW_ID, event);

            // then
            assertThat(delivered).isTrue();
            assertThat(loads).containsExactly(WORKFLOW_ID);
            assertThat(liveWorkflows.find(WORKFLOW_ID)).hasValueSatisfying(
                    woken -> assertThat(((InspectionCountingWorkflowExecution) woken).deliveredEvents())
                            .containsExactly(event)
            );
        }

        @Test
        void deliversNothingToAWorkflowThatIsNotLive() {
            // when
            boolean delivered = deliver(WORKFLOW_ID, paid("1"));

            // then
            assertThat(delivered).isFalse();
        }
    }

    @Nested
    class CancellationRequests {

        private final WorkflowCancellationService service =
                new WorkflowCancellationService(liveWorkflows, () -> support);

        @Test
        void failARequestThatAWokenExecutionTakesNoTaskForAndLeaveNothingQueuedOnIt() {
            // given - a woken execution whose task queue is full
            var hibernated = awaitingPayment(WORKFLOW_ID, "1");
            adopt(hibernated);
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            var queueFull = new IllegalStateException("The task queue is full.");
            execution.rejectTasksWith(queueFull);
            assertThat(wake(hibernated, execution)).isTrue();

            // when
            var request = service.requestStepCancellation(WORKFLOW_ID, "step", null);

            // then - the request fails with the reason it could not be queued
            assertThat(request).failsWithin(TIMEOUT).withThrowableThat().havingRootCause().isSameAs(queueFull);
            // and is not made again once the execution swaps back for its hibernation, which would wake the workflow
            assertThat(hibernate(execution, awaitingPayment(WORKFLOW_ID, "1"))).isTrue();
            assertThat(loads).isEmpty();
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            if (!latch.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                throw new IllegalStateException("The latch was not released in time.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /**
     * Keeps the executions like the default repository, and runs given actions once: right before it removes the next
     * execution, which is the moment a leave or a hibernation of the registry is in progress, and around the next, or a
     * given, listing of the executions of a segment, which is the moment a broadcast looks up its addressees, see
     * {@link LiveWorkflows#broadcast}. The tests of the registry list the executions of a segment only through a
     * broadcast, so every listing this counts is one of a broadcast.
     */
    private static final class HookRepository extends InMemoryWorkflowExecutionRepository {

        private final AtomicReference<@Nullable Runnable> beforeNextRemoval = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> beforeNextListing = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> afterNextListing = new AtomicReference<>();
        private final Map<Integer, Runnable> beforeListings = new ConcurrentHashMap<>();
        private final AtomicInteger listings = new AtomicInteger();

        private void beforeNextRemoval(Runnable action) {
            beforeNextRemoval.set(action);
        }

        private void aroundNextListing(Runnable before, Runnable after) {
            afterNextListing.set(after);
            beforeNextListing.set(before);
        }

        private void beforeListing(int listing, Runnable action) {
            beforeListings.put(listing, action);
        }

        private int listings() {
            return listings.get();
        }

        private static void runOnce(AtomicReference<@Nullable Runnable> hook) {
            var action = hook.getAndSet(null);
            if (action != null) {
                action.run();
            }
        }

        @Override
        public boolean remove(String workflowId, WorkflowExecution execution) {
            runOnce(beforeNextRemoval);
            return super.remove(workflowId, execution);
        }

        @Override
        public Set<WorkflowExecution> findAllOwnedBy(@Nullable Segment segment) {
            var listing = listings.incrementAndGet();
            runOnce(beforeNextListing);
            var action = beforeListings.remove(listing);
            if (action != null) {
                action.run();
            }
            var executions = super.findAllOwnedBy(segment);
            runOnce(afterNextListing);
            return executions;
        }
    }
}
