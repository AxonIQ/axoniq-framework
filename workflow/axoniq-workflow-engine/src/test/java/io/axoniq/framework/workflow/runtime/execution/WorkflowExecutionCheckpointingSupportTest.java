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
import org.junit.jupiter.api.*;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link WorkflowExecutionCheckpointingSupport}.
 */
class WorkflowExecutionCheckpointingSupportTest {

    @Test
    void coalescesCallbacksUntilTheInlineBarrierIsConsumed() {
        var taskQueue = new InlineExecutionTaskQueue();
        var support = new WorkflowExecutionCheckpointingSupport(taskQueue, CheckpointWorkStateListener.NO_OP);
        var callbacks = new AtomicInteger();

        support.addCheckpointLatch(callbacks::incrementAndGet);
        support.addCheckpointLatch(callbacks::incrementAndGet);

        assertThat(taskQueue.tasks).hasSize(1);
        assertThat(support.hasUnsafeCheckpointWork()).isTrue();

        support.runTask(taskQueue.tasks.remove(), mock(WorkflowExecution.class));

        assertThat(callbacks).hasValue(2);
        assertThat(support.hasUnsafeCheckpointWork()).isFalse();
    }

    @Test
    void invokesTheCallbackImmediatelyWhenTheTaskQueueIsNotRunning() {
        var taskQueue = new InlineExecutionTaskQueue();
        taskQueue.running = false;
        var support = new WorkflowExecutionCheckpointingSupport(taskQueue, CheckpointWorkStateListener.NO_OP);
        var callbacks = new AtomicInteger();

        support.addCheckpointLatch(callbacks::incrementAndGet);

        assertThat(callbacks).hasValue(1);
        assertThat(taskQueue.tasks).isEmpty();
        assertThat(support.hasUnsafeCheckpointWork()).isFalse();
    }

    @Test
    void queuesTheLatchInsteadOfFiringImmediatelyWhenNotRunningButWorkIsQueued() {
        var taskQueue = new InlineExecutionTaskQueue();
        taskQueue.running = false;
        taskQueue.tasks.add(ignored -> {
        });
        var support = new WorkflowExecutionCheckpointingSupport(taskQueue, CheckpointWorkStateListener.NO_OP);
        var callbacks = new AtomicInteger();

        support.addCheckpointLatch(callbacks::incrementAndGet);

        assertThat(callbacks).hasValue(0);
        assertThat(taskQueue.tasks).hasSize(2);

        support.runTask(taskQueue.tasks.remove(), mock(WorkflowExecution.class));
        assertThat(callbacks).hasValue(0);

        support.runTask(taskQueue.tasks.remove(), mock(WorkflowExecution.class));
        assertThat(callbacks).hasValue(1);
    }

    @Test
    void reportsCheckpointWorkStateTransitions() {
        var taskQueue = new InlineExecutionTaskQueue();
        var unsafeTransitions = new AtomicInteger();
        var safeTransitions = new AtomicInteger();
        var support = new WorkflowExecutionCheckpointingSupport(
                taskQueue,
                new CheckpointWorkStateListener() {
                    @Override
                    public void onMarkedUnsafe() {
                        unsafeTransitions.incrementAndGet();
                    }

                    @Override
                    public void onMarkedSafe() {
                        safeTransitions.incrementAndGet();
                    }
                }
        );

        support.appendTask(ignored -> {
        });

        assertThat(unsafeTransitions).hasValue(1);
        assertThat(safeTransitions).hasValue(0);

        support.runTask(taskQueue.tasks.remove(), mock(WorkflowExecution.class));

        assertThat(safeTransitions).hasValue(1);
    }

    @Nested
    class PendingCheckpointLatchCallbacks {

        // Large enough that running the callbacks as a nested call chain would overflow the stack.
        private static final int CALLBACK_COUNT = 100_000;

        @Test
        void runsEveryCallbackOnceInRegistrationOrderWhenManyCallbacksShareOnePendingLatch() {
            // given
            var taskQueue = new InlineExecutionTaskQueue();
            var support = new WorkflowExecutionCheckpointingSupport(taskQueue, CheckpointWorkStateListener.NO_OP);
            List<Integer> invoked = new ArrayList<>();
            for (int i = 0; i < CALLBACK_COUNT; i++) {
                int index = i;
                support.addCheckpointLatch(() -> invoked.add(index));
            }
            assertThat(taskQueue.tasks).hasSize(1);
            assertThat(support.hasUnsafeCheckpointWork()).isTrue();

            // when
            support.runTask(taskQueue.tasks.remove(), mock(WorkflowExecution.class));

            // then
            assertThat(invoked).containsExactlyElementsOf(expectedIndices());
            assertThat(support.hasUnsafeCheckpointWork()).isFalse();
        }

        @Test
        void completesManyPendingCallbacksInRegistrationOrderWhenThePendingLatchIsReleased() {
            // given
            var taskQueue = new InlineExecutionTaskQueue();
            var support = new WorkflowExecutionCheckpointingSupport(taskQueue, CheckpointWorkStateListener.NO_OP);
            List<Integer> invoked = new ArrayList<>();
            for (int i = 0; i < CALLBACK_COUNT; i++) {
                int index = i;
                support.addCheckpointLatch(() -> invoked.add(index));
            }
            assertThat(taskQueue.tasks).hasSize(1);

            // when
            support.completePendingCheckpointLatch();

            // then
            assertThat(invoked).containsExactlyElementsOf(expectedIndices());
        }

        @Test
        void runsCallbacksRegisteredAfterConsumptionOnTheNextLatch() {
            // given
            var taskQueue = new InlineExecutionTaskQueue();
            var support = new WorkflowExecutionCheckpointingSupport(taskQueue, CheckpointWorkStateListener.NO_OP);
            var first = new AtomicInteger();
            var second = new AtomicInteger();
            support.addCheckpointLatch(first::incrementAndGet);
            support.runTask(taskQueue.tasks.remove(), mock(WorkflowExecution.class));

            // when
            support.addCheckpointLatch(second::incrementAndGet);

            // then the second callback waits for a freshly queued latch
            assertThat(first).hasValue(1);
            assertThat(second).hasValue(0);
            assertThat(taskQueue.tasks).hasSize(1);

            // when
            support.runTask(taskQueue.tasks.remove(), mock(WorkflowExecution.class));

            // then
            assertThat(first).hasValue(1);
            assertThat(second).hasValue(1);
        }

        private static List<Integer> expectedIndices() {
            return IntStream.range(0, CALLBACK_COUNT).boxed().toList();
        }
    }

    private static final class InlineExecutionTaskQueue
            implements WorkflowExecutionCheckpointingSupport.ExecutionTaskQueue {

        private final Queue<Consumer<WorkflowExecution>> tasks = new ArrayDeque<>();
        private boolean running = true;

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public boolean hasQueuedTasks() {
            return !tasks.isEmpty();
        }

        @Override
        public void appendTask(Consumer<WorkflowExecution> task) {
            tasks.add(task);
        }
    }
}
