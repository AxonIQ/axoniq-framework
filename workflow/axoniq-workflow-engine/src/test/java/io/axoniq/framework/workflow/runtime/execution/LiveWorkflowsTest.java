/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution.CheckpointWorkStateListener;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.framework.workflow.runtime.execution.SegmentTestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Tests the transitions of {@link LiveWorkflows} that bring a workflow into memory and take it out again, and the safe
 * points they track, which only the execution in memory may report.
 *
 * @author Allard Buijze
 */
class LiveWorkflowsTest {

    private static final Segment SEGMENT = FOUR_SEGMENTS.getFirst();
    private static final String WORKFLOW_ID = anyIdOn(SEGMENT);
    private static final String FOREIGN_ID = idOnAnotherSegmentThan(WORKFLOW_ID);

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final RemovalHookRepository repository = new RemovalHookRepository();
    private final LiveWorkflows liveWorkflows = new LiveWorkflows(repository);

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
            WorkflowExecution entered = liveWorkflows.enter(WORKFLOW_ID, () -> execution);

            // then
            assertThat(entered).isSameAs(execution);
            assertThat(liveWorkflows.isInMemory(execution)).isTrue();
            assertThat(liveWorkflows.find(WORKFLOW_ID)).containsSame(execution);
            assertThat(repository.findById(WORKFLOW_ID)).containsSame(execution);
            assertThat(liveWorkflows.inMemory()).containsExactly(execution);
            assertThat(execution.checkpointWorkStateListener())
                    .as("an execution in memory reports its safe points")
                    .isNotNull();
        }

        @Test
        void returnsTheExecutionInMemoryWithoutCreatingAnother() {
            // given
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, () -> execution);

            // when
            WorkflowExecution entered = liveWorkflows.enter(WORKFLOW_ID, () -> {
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
            assertThatThrownBy(() -> liveWorkflows.enter(WORKFLOW_ID, () -> execution))
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
            assertThatThrownBy(() -> liveWorkflows.enter(WORKFLOW_ID, () -> failed))
                    .isInstanceOf(IllegalStateException.class);
            repository.remove(WORKFLOW_ID, stored);
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);

            // when
            WorkflowExecution entered = liveWorkflows.enter(WORKFLOW_ID, () -> execution);

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
            liveWorkflows.enter(WORKFLOW_ID, () -> execution);

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
            liveWorkflows.enter(WORKFLOW_ID, () -> departed);
            liveWorkflows.leave(departed);
            var successor = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, () -> successor);

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
            liveWorkflows.enter(WORKFLOW_ID, () -> departing);
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
                    () -> entered.complete(liveWorkflows.enter(WORKFLOW_ID, () -> successor))
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
            liveWorkflows.enter(WORKFLOW_ID, () -> execution);
            safePointsOf(execution).onMarkedUnsafe();

            // when
            liveWorkflows.clear();

            // then
            assertThat(liveWorkflows.find(WORKFLOW_ID)).isEmpty();
            assertThat(repository.findAll()).isEmpty();
            assertThat(liveWorkflows.unsafeOf(SEGMENT)).isEmpty();
        }
    }

    @Nested
    class SafePoints {

        @Test
        void listsTheUnsafeExecutionsOfTheGivenSegmentOnly() {
            // given
            var owned = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            var foreign = new InspectionCountingWorkflowExecution(FOREIGN_ID);
            liveWorkflows.enter(WORKFLOW_ID, () -> owned);
            liveWorkflows.enter(FOREIGN_ID, () -> foreign);

            // when
            safePointsOf(owned).onMarkedUnsafe();
            safePointsOf(foreign).onMarkedUnsafe();

            // then
            assertThat(liveWorkflows.unsafeOf(SEGMENT))
                    .extracting(LiveWorkflows.InMemory::execution)
                    .containsExactly(owned);
        }

        @Test
        void forgetsTheUnsafeMarkOfAnExecutionThatMarksItselfSafe() {
            // given
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, () -> execution);
            safePointsOf(execution).onMarkedUnsafe();

            // when
            safePointsOf(execution).onMarkedSafe();

            // then
            assertThat(liveWorkflows.unsafeOf(SEGMENT)).isEmpty();
        }

        @Test
        void forgetsTheUnsafeMarkOfAnExecutionThatLeavesMemory() {
            // given
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, () -> execution);
            safePointsOf(execution).onMarkedUnsafe();

            // when
            liveWorkflows.leave(execution);

            // then
            assertThat(liveWorkflows.unsafeOf(SEGMENT)).isEmpty();
        }

        @Test
        void forgetsAMarkUnsafeThatArrivesWhileTheExecutionLeavesMemory() {
            // given - an execution that reports unsafe work while its leave removes it from the repository
            var execution = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, () -> execution);
            var unsafeDuringLeave = new AtomicReference<Integer>();
            repository.beforeNextRemoval(() -> {
                safePointsOf(execution).onMarkedUnsafe();
                unsafeDuringLeave.set(liveWorkflows.unsafeOf(SEGMENT).size());
            });

            // when
            liveWorkflows.leave(execution);

            // then - the mark was taken while the execution was still in memory, and its leave removed it
            assertThat(unsafeDuringLeave).hasValue(1);
            assertThat(liveWorkflows.unsafeOf(SEGMENT)).isEmpty();
        }

        @Test
        void ignoresALateMarkUnsafeOfAnExecutionThatLeftMemory() {
            // given - an execution that left memory, and its successor, which has no unsafe work
            var departed = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, () -> departed);
            liveWorkflows.leave(departed);
            var successor = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, () -> successor);
            safePointsOf(successor).onMarkedSafe();

            // when - the execution that left keeps unwinding, and reports unsafe work
            safePointsOf(departed).onMarkedUnsafe();

            // then
            assertThat(liveWorkflows.unsafeOf(SEGMENT))
                    .as("an execution that left memory must not mark its successor's workflow unsafe")
                    .isEmpty();
        }

        @Test
        void keepsTheUnsafeMarkOfTheSuccessorWhenTheExecutionThatLeftMemoryReportsLate() {
            // given - an execution that left memory while unsafe, and its successor, which is unsafe as well
            var departed = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, () -> departed);
            safePointsOf(departed).onMarkedUnsafe();
            liveWorkflows.leave(departed);
            var successor = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, () -> successor);
            safePointsOf(successor).onMarkedUnsafe();

            // when - the execution that left keeps unwinding, and reports both unsafe and safe work
            safePointsOf(departed).onMarkedUnsafe();
            safePointsOf(departed).onMarkedSafe();

            // then
            assertThat(liveWorkflows.unsafeOf(SEGMENT))
                    .as("an execution that left memory must not replace or clear its successor's unsafe mark")
                    .extracting(LiveWorkflows.InMemory::execution)
                    .containsExactly(successor);
        }

        @Test
        void markingAnEntrySafeLeavesTheUnsafeMarkOfTheEntryThatReplacedIt() {
            // given - the entry of an unsafe execution, read before it left memory, and its unsafe successor
            var departed = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, () -> departed);
            safePointsOf(departed).onMarkedUnsafe();
            var departedEntry = liveWorkflows.unsafeOf(SEGMENT).getFirst();
            liveWorkflows.leave(departed);
            var successor = new InspectionCountingWorkflowExecution(WORKFLOW_ID);
            liveWorkflows.enter(WORKFLOW_ID, () -> successor);
            safePointsOf(successor).onMarkedUnsafe();

            // when - a checkpoint that read the entry before it left finds its execution safe
            liveWorkflows.markSafe(departedEntry);

            // then
            assertThat(liveWorkflows.unsafeOf(SEGMENT))
                    .extracting(LiveWorkflows.InMemory::execution)
                    .containsExactly(successor);
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
     * Keeps the executions like the default repository, and runs a given action once, right before it removes the
     * next execution, which is the moment a leave of the registry is in progress.
     */
    private static final class RemovalHookRepository extends InMemoryWorkflowExecutionRepository {

        private final AtomicReference<@Nullable Runnable> beforeNextRemoval = new AtomicReference<>();

        private void beforeNextRemoval(Runnable action) {
            beforeNextRemoval.set(action);
        }

        @Override
        public boolean remove(String workflowId, WorkflowExecution execution) {
            var action = beforeNextRemoval.getAndSet(null);
            if (action != null) {
                action.run();
            }
            return super.remove(workflowId, execution);
        }
    }
}
