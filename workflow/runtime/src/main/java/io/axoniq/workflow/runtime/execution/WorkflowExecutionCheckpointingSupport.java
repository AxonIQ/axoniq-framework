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
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution.CheckpointWorkStateListener;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.NonNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Encapsulates the checkpoint latch mechanics for a single {@link WorkflowExecution}.
 * <p>
 * The {@link WorkflowEngine} does not store a
 * {@link org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken} merely because an event
 * was seen. Instead, token advancement is delayed until the execution has proved that every workflow task that was
 * queued before the checkpoint request has been observed by the workflow thread. This class implements that
 * <b>latch</b>.
 * <p>
 * The model is intentionally small:
 * <ul>
 *     <li>The {@code WorkflowExecution} owns a single task queue that serializes all workflow mutations.</li>
 *     <li>A checkpoint request is represented as a synthetic task, {@link CheckpointLatch}, appended to that same
 *     queue.</li>
 *     <li>When the workflow thread consumes that synthetic task, the support knows that all previously queued work has
 *     already been executed, because queue order is preserved.</li>
 *     <li>Only then is the callback associated with the checkpoint request invoked, allowing the engine to re-check
 *     whether it is now safe to complete {@link io.axoniq.framework.messaging.eventstreaming.checkpoint.Checkpointing#onCheckpointAdvanced(Segment, TrackingToken)}.</li>
 * </ul>
 * <p>
 * This class does <em>not</em> decide <em>when</em> the processor requests a checkpoint. That decision stays in
 * {@code WorkflowEngine}. Its job is narrower: once the engine wants a latch, this class inserts that latch into
 * the execution queue and reports when the queue has advanced past it.
 * <p>
 * The implementation relies on two important constraints of the surrounding protocol:
 * <ul>
 *     <li>There is at most one active checkpoint advance per segment at a time. The processor does not call
 *     {@code onCheckpointAdvanced(...)} concurrently for the same segment.</li>
 *     <li>Multiple "please notify me when drained" requests may still happen before the already-queued latch is
 *     consumed. Those requests are coalesced here onto the same queued {@link CheckpointLatch} instead of enqueueing
 *     multiple identical latch tasks.</li>
 * </ul>
 * <p>
 * The state tracked here has the following meaning:
 * <ul>
 *     <li>{@code taskActive} is {@code true} while the workflow thread is currently executing a task obtained from the
 *     queue. A checkpoint is not safe while such a task is still running, even if the queue itself is empty.</li>
 *     <li>{@code latchQueued} is {@code true} once a {@link CheckpointLatch} has been appended and until
 *     that intent is consumed by the workflow thread.</li>
 *     <li>{@code latchCallback} is the callback to run after the queued latch has been crossed. If several
 *     callers register while the same intent is pending, their callbacks are composed into one runnable.</li>
 * </ul>
 * <p>
 * The typical sequence is:
 * <ol>
 *     <li>The engine wants to know when the execution queue has drained up to the current point.</li>
 *     <li>{@link #addCheckpointLatch(Runnable)} either reuses the already queued latch or appends a new
 *     {@link CheckpointLatch} to the execution queue.</li>
 *     <li>The execution eventually dequeues that intent and runs it through {@link #runTask(Consumer, WorkflowExecution)}.
 *     Consuming the intent means every task that was ahead of it has completed.</li>
 *     <li>After the task finishes, the stored {@code latchCallback} is invoked.</li>
 *     <li>The engine re-evaluates whether any work is still unsafe. If more workflow work was appended after the
 *     latch, it can schedule another one and waits again.</li>
 * </ol>
 * <p>
 * Synchronization is deliberately local to this instance. The host execution already provides the single-threaded task
 * queue; this class only needs to protect its own bookkeeping about whether such a latch has been queued and
 * which callback should fire when it is crossed.
 * <p>
 * The queued-latch state and composed callback are guarded by this instance's monitor. {@code taskActive} is an
 * {@link AtomicBoolean} because it is set around arbitrary workflow task execution, which must not hold that monitor,
 * while checkpoint safety may be queried concurrently. Latch callbacks are captured while coordinated but always run
 * after task execution and outside the monitor.
 *
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 1.0.0
 */
@Internal
final class WorkflowExecutionCheckpointingSupport {

    private static final Runnable NO_OP = () -> {
    };

    private final ExecutionTaskQueue executionTaskQueue;
    private final AtomicBoolean taskActive = new AtomicBoolean(false);
    private boolean latchQueued;
    private Runnable latchCallback = NO_OP;

    private CheckpointWorkStateListener checkpointWorkStateListener;
    private boolean checkpointWorkUnsafe;

    /**
     * Creates checkpoint support bound to a {@link WorkflowExecution} task queue.
     *
     * @param executionTaskQueue task queue used to inspect queue state and append checkpoint latches
     */
    @Internal
    WorkflowExecutionCheckpointingSupport(ExecutionTaskQueue executionTaskQueue,
                                          CheckpointWorkStateListener checkpointWorkStateListener) {
        this.executionTaskQueue = requireNonNull(executionTaskQueue, "The CheckpointLatchQueue must not be null.");
        this.checkpointWorkStateListener = requireNonNull(
                checkpointWorkStateListener, "Checkpoint work state listener must not be null"
        );
    }

    /**
     * Requests notification once the execution queue has crossed the current checkpoint latch, acting as an immediate
     * delegate from an {@link WorkflowExecution#addCheckpointLatch(Runnable)} invocation.
     * <p>
     * If the execution is not live yet, the callback is invoked immediately because no asynchronous task queue is
     * active. Otherwise, this method ensures that exactly one {@link CheckpointLatch} is queued for the current drain
     * cycle.
     * <p>
     * Repeated calls while that same intent is still pending do not enqueue more tasks. Instead, their callbacks are
     * composed into {@link #latchCallback} so they all fire when the single queued latch is consumed.
     * <p>
     * The latch is appended outside the synchronized block. The synchronized section only decides whether a new latch
     * is needed; once that decision is made, queueing the task does not need to hold this object's monitor.
     *
     * @param latch callback invoked after the queued checkpoint latch has been consumed
     */
    public void addCheckpointLatch(@NonNull Runnable latch) {
        requireNonNull(latch, "The when-complete runnable must not be null");
        if (!executionTaskQueue.isRunning()) {
            latch.run();
            return;
        }

        synchronized (this) {
            Runnable previous = latchCallback;
            latchCallback = previous == NO_OP ? latch : () -> {
                previous.run();
                latch.run();
            };
            if (latchQueued) {
                return;
            }
            latchQueued = true;
            reportCheckpointWorkUnsafe();
        }

        try {
            executionTaskQueue.appendTask(new CheckpointLatch());
        } catch (RuntimeException e) {
            synchronized (this) {
                latchQueued = false;
                reportCheckpointWorkState();
            }
            throw e;
        }
    }

    /**
     * Determines whether a checkpoint would still be unsafe for this execution, acting as an immediate delegate from an
     * {@link WorkflowExecution#hasUnsafeCheckpointWork()} invocation.
     * <p>
     * A checkpoint remains unsafe while any of the following is true:
     * <ul>
     *     <li>a workflow task is currently executing,</li>
     *     <li>ordinary workflow tasks remain queued, or</li>
     *     <li>a {@link CheckpointLatch} has been queued but not yet consumed.</li>
     * </ul>
     * <p>
     * When the execution is not live, the answer is always {@code false} because there is no asynchronous queue work
     * to wait for.
     *
     * @return {@code true} if checkpoint advancement must still wait, otherwise {@code false}
     */
    public boolean hasUnsafeCheckpointWork() {
        synchronized (this) {
            return unsafe();
        }
    }

    /**
     * Whether this execution currently makes checkpoint advancement unsafe.
     * <p>
     * Deliberately independent of whether the body is running: a materialized but not started execution accumulates
     * queued work that nothing drains until its body starts, and excusing it would let the segment token pass an event
     * whose effect is still sitting in that queue.
     */
    private boolean unsafe() {
        return taskActive.get() || executionTaskQueue.hasQueuedTasks() || latchQueued;
    }

    /**
     * Synchronized registration of the given {@code listner}, acting as an immediate delegate from an
     * {@link WorkflowExecution#registerCheckpointWorkStateListener(CheckpointWorkStateListener)} invocation.
     *
     * @param listener the {@code CheckpointWorkStateListener} to register
     */
    public synchronized void registerListener(CheckpointWorkStateListener listener) {
        checkpointWorkStateListener = requireNonNull(listener, "Checkpoint work state listener must not be null");
        checkpointWorkUnsafe = unsafe();
        if (checkpointWorkUnsafe) {
            checkpointWorkStateListener.onMarkedUnsafe();
        } else {
            checkpointWorkStateListener.onMarkedSafe();
        }
    }

    synchronized void refreshCheckpointWorkState() {
        reportCheckpointWorkState();
    }

    void appendTask(@NonNull Consumer<WorkflowExecution> task) {
        synchronized (this) {
            reportCheckpointWorkUnsafe();
            try {
                executionTaskQueue.appendTask(task);
            } catch (RuntimeException e) {
                reportCheckpointWorkState();
                throw e;
            }
        }
    }

    private void reportCheckpointWorkUnsafe() {
        if (checkpointWorkUnsafe) {
            return;
        }
        checkpointWorkUnsafe = true;
        checkpointWorkStateListener.onMarkedUnsafe();
    }

    /**
     * Executes one queue task while maintaining checkpoint bookkeeping.
     * <p>
     * This method marks a task as active for the duration of execution so {@link #hasUnsafeCheckpointWork()} can
     * observe that the queue is not yet safe even when it appears empty.
     * <p>
     * When the task is a {@link CheckpointLatch}, consuming it snapshots the currently composed callback. That callback
     * is then invoked after the task finishes and after {@code taskActive} has been cleared, so the subsequent safety
     * re-check sees the most up-to-date state.
     *
     * @param task      the queue task to run
     * @param execution the execution instance passed to the task
     */
    void runTask(@NonNull Consumer<WorkflowExecution> task,
                 @NonNull WorkflowExecution execution) {
        Runnable afterTask;
        taskActive.set(true);
        synchronized (this) {
            reportCheckpointWorkUnsafe();
        }
        try {
            task.accept(execution);
            afterTask = task instanceof CheckpointLatch checkpointLatch ? checkpointLatch.whenComplete() : null;
        } finally {
            taskActive.set(false);
            synchronized (this) {
                reportCheckpointWorkState();
            }
        }
        if (afterTask != null) {
            afterTask.run();
        }
    }

    private void reportCheckpointWorkState() {
        var unsafe = unsafe();
        if (unsafe == checkpointWorkUnsafe) {
            return;
        }
        checkpointWorkUnsafe = unsafe;
        if (unsafe) {
            checkpointWorkStateListener.onMarkedUnsafe();
        } else {
            checkpointWorkStateListener.onMarkedSafe();
        }
    }

    /**
     * Indicates whether the given queued task is a checkpoint barrier owned by this support instance.
     *
     * @param task queued workflow task to inspect
     * @return {@code true} if the task is a checkpoint barrier
     */
    boolean isCheckpointLatch(@Nonnull Consumer<WorkflowExecution> task) {
        return task instanceof CheckpointLatch;
    }

    /**
     * Completes any checkpoint callbacks whose latches cannot be consumed because the workflow driver has stopped.
     * <p>
     * Once the driver is no longer executable, no further queue work can make checkpoint advancement unsafe. Releasing
     * the callbacks prevents a checkpoint waiter from being stranded by terminal cleanup.
     */
    void completePendingCheckpointLatch() {
        Runnable callback;
        synchronized (this) {
            checkpointWorkUnsafe = false;
            callback = latchCallback;
            latchCallback = NO_OP;
        }
        callback.run();
    }

    /**
     * Task queue containing <b>any</b> {@link WorkflowExecution} task, as well as
     * {@link CheckpointLatch CheckpointLatches}.
     * <p>
     * The support deliberately knows nothing about queue implementation details, replay state, or execution internals.
     * It only asks the task queue the minimal questions required to place and observe a {@code CheckpointLatch}.
     */
    interface ExecutionTaskQueue {

        /**
         * Indicates whether the execution is currently processing through its live task queue.
         * <p>
         * When the execution is not running yet, there is no concurrent queue-drain problem to coordinate with, so
         * checkpoint latches can be completed immediately.
         *
         * @return {@code true} if queue-based execution is running, otherwise {@code false}
         */
        boolean isRunning();

        /**
         * Reports whether the execution still has queued tasks waiting behind the currently running one.
         * <p>
         * This is part of the checkpoint safety test: even if no checkpoint intent is queued, a checkpoint is not yet
         * safe while ordinary workflow tasks are still waiting in the queue.
         *
         * @return {@code true} if tasks remain queued, otherwise {@code false}
         */
        boolean hasQueuedTasks();

        /**
         * Appends a task to the execution queue.
         * <p>
         * {@link WorkflowExecutionCheckpointingSupport} uses this to inject {@link CheckpointLatch CheckpointLatchs}
         * into the exact same queue that carries normal workflow work, which is what makes the latch meaningful.
         *
         * @param task the task to append
         */
        void appendTask(@NonNull Consumer<WorkflowExecution> task);
    }

    /**
     * Synthetic {@link ExecutionTaskQueue} task that acts as the checkpoint latch.
     * <p>
     * The task itself does not mutate workflow state. Its purpose is positional: once it reaches the head of the queue,
     * every task that was queued before the checkpoint request has already run. At that moment it captures and clears
     * the currently accumulated latch callback attached through {@link #addCheckpointLatch} so that the callback can be
     * invoked after the task completes.
     */
    private final class CheckpointLatch implements Consumer<WorkflowExecution> {

        private Runnable whenComplete = NO_OP;

        /**
         * Marks the queued checkpoint latch as consumed and captures the callback associated with that latch.
         * <p>
         * This runs on the workflow thread as part of normal queue consumption. Clearing {@link #latchQueued} here
         * allows a later checkpoint cycle to enqueue a fresh latch if new work appears after this point.
         *
         * @param ignored the workflow execution, unused because this task is just a latch to proceed with a
         *                checkpointing request
         */
        @Override
        public void accept(WorkflowExecution ignored) {
            synchronized (WorkflowExecutionCheckpointingSupport.this) {
                latchQueued = false;
                whenComplete = latchCallback;
                latchCallback = NO_OP;
            }
        }

        /**
         * Returns the callback to run after the checkpoint task has finished.
         *
         * @return the callback to run after the checkpoint task has finished
         */
        private Runnable whenComplete() {
            return whenComplete;
        }
    }
}
