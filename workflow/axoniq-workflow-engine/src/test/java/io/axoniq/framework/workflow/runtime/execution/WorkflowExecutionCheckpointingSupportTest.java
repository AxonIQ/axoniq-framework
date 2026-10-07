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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution.CheckpointWorkStateListener;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.junit.jupiter.api.*;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    void manyCallbacksAddedWhileALatchIsQueuedAllRunWhenItIsConsumed() {
        // given
        var taskQueue = new InlineExecutionTaskQueue();
        var support = new WorkflowExecutionCheckpointingSupport(taskQueue, CheckpointWorkStateListener.NO_OP);
        var callbacks = new AtomicInteger();
        for (int i = 0; i < 100_000; i++) {
            support.addCheckpointLatch(callbacks::incrementAndGet);
        }

        // when
        support.runTask(taskQueue.tasks.remove(), mock(WorkflowExecution.class));

        // then
        assertThat(callbacks).hasValue(100_000);
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

        support.appendDeliveryTask(ignored -> {
        }, null);

        assertThat(unsafeTransitions).hasValue(1);
        assertThat(safeTransitions).hasValue(0);

        support.runTask(taskQueue.tasks.remove(), mock(WorkflowExecution.class));

        assertThat(safeTransitions).hasValue(1);
    }

    @Test
    void aTaskTakenFromTheQueueButNotYetRunKeepsTheWorkUnsafe() {
        // given
        var taskQueue = new InlineExecutionTaskQueue();
        var support = new WorkflowExecutionCheckpointingSupport(taskQueue, CheckpointWorkStateListener.NO_OP);
        support.appendDeliveryTask(ignored -> {
        }, null);

        // when the driver takes the task, before it runs it
        var taken = taskQueue.tasks.remove();

        // then
        assertThat(support.hasUnsafeCheckpointWork()).isTrue();
        support.runTask(taken, mock(WorkflowExecution.class));
        assertThat(support.hasUnsafeCheckpointWork()).isFalse();
    }

    @Test
    void aTaskThatFailsKeepsTheWorkUnsafe() {
        // given
        var taskQueue = new InlineExecutionTaskQueue();
        var support = new WorkflowExecutionCheckpointingSupport(taskQueue, CheckpointWorkStateListener.NO_OP);
        support.appendDeliveryTask(ignored -> {
            throw new IllegalStateException("the store refused the append");
        }, null);

        // when
        var taken = taskQueue.tasks.remove();
        assertThatThrownBy(() -> support.runTask(taken, mock(WorkflowExecution.class)))
                .isInstanceOf(IllegalStateException.class);

        // then
        assertThat(support.hasUnsafeCheckpointWork()).isTrue();
    }

    @Test
    void queuedWorkThatIsDroppedWithoutRunningKeepsTheWorkUnsafe() {
        // given
        var taskQueue = new InlineExecutionTaskQueue();
        var support = new WorkflowExecutionCheckpointingSupport(taskQueue, CheckpointWorkStateListener.NO_OP);
        support.appendDeliveryTask(ignored -> {
        }, null);

        // when the queue is cleared, as a pausing execution does
        taskQueue.tasks.clear();

        // then
        assertThat(support.hasUnsafeCheckpointWork()).isTrue();
    }

    @Test
    void workThatHasRunHoldsNoCheckpointAtItsPosition() {
        // given
        var taskQueue = new InlineExecutionTaskQueue();
        var support = new WorkflowExecutionCheckpointingSupport(taskQueue, CheckpointWorkStateListener.NO_OP);
        var position = new GlobalSequenceTrackingToken(5);
        support.appendDeliveryTask(ignored -> {
        }, position);

        // when
        support.runTask(taskQueue.tasks.remove(), mock(WorkflowExecution.class));

        // then
        assertThat(support.holdsCheckpoint(position)).isFalse();
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
