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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link WorkflowExecutionCheckpointSupport}.
 */
class WorkflowExecutionCheckpointSupportTest {

    @Test
    void coalescesCallbacksUntilTheInlineBarrierIsConsumed() {
        var taskQueue = new InlineCheckpointBarrierTaskQueue();
        var support = new WorkflowExecutionCheckpointSupport(taskQueue);
        var callbacks = new AtomicInteger();

        support.appendCheckpointIntent(callbacks::incrementAndGet);
        support.appendCheckpointIntent(callbacks::incrementAndGet);

        assertThat(taskQueue.tasks).hasSize(1);
        assertThat(support.hasUnsafeCheckpointWork()).isTrue();

        support.runTask(taskQueue.tasks.remove(), mock(WorkflowExecution.class));

        assertThat(callbacks).hasValue(2);
        assertThat(support.hasUnsafeCheckpointWork()).isFalse();
    }

    @Test
    void invokesTheCallbackImmediatelyWhenTheTaskQueueIsNotRunning() {
        var taskQueue = new InlineCheckpointBarrierTaskQueue();
        taskQueue.running = false;
        var support = new WorkflowExecutionCheckpointSupport(taskQueue);
        var callbacks = new AtomicInteger();

        support.appendCheckpointIntent(callbacks::incrementAndGet);

        assertThat(callbacks).hasValue(1);
        assertThat(taskQueue.tasks).isEmpty();
        assertThat(support.hasUnsafeCheckpointWork()).isFalse();
    }

    @Test
    void reportsCheckpointWorkStateTransitions() {
        var taskQueue = new InlineCheckpointBarrierTaskQueue();
        var unsafeTransitions = new AtomicInteger();
        var safeTransitions = new AtomicInteger();
        var support = new WorkflowExecutionCheckpointSupport(
                taskQueue,
                new WorkflowExecution.CheckpointWorkStateListener() {
                    @Override
                    public void onCheckpointWorkBecameUnsafe() {
                        unsafeTransitions.incrementAndGet();
                    }

                    @Override
                    public void onCheckpointWorkBecameSafe() {
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

    private static final class InlineCheckpointBarrierTaskQueue
            implements WorkflowExecutionCheckpointSupport.CheckpointBarrierTaskQueue {

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
