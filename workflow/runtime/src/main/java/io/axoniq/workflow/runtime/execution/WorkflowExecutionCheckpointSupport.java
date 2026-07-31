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
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Encapsulates the checkpoint barrier mechanics for a single {@link SimpleWorkflowExecution}.
 * <p>
 * The workflow engine does not store a tracking token merely because an event was seen. Instead, token advancement is
 * delayed until the execution has proved that every workflow task that was queued before the checkpoint request has
 * been observed by the workflow thread. This class implements that barrier.
 * <p>
 * The model is intentionally small:
 * <ul>
 *     <li>The workflow execution owns a single task queue that serializes all workflow mutations.</li>
 *     <li>A checkpoint request is represented as a synthetic task, {@link CheckpointIntent}, appended to that same
 *     queue.</li>
 *     <li>When the workflow thread consumes that synthetic task, the support knows that all previously queued work has
 *     already been executed, because queue order is preserved.</li>
 *     <li>Only then is the callback associated with the checkpoint request invoked, allowing the engine to re-check
 *     whether it is now safe to complete {@code onCheckpointAdvanced(...)}.</li>
 * </ul>
 * <p>
 * This class does <em>not</em> decide <em>when</em> the processor requests a checkpoint. That decision stays in
 * {@code WorkflowEngine}. Its job is narrower: once the engine wants a barrier, this class inserts that barrier into
 * the execution queue and reports when the queue has advanced past it.
 * <p>
 * The implementation relies on two important constraints of the surrounding protocol:
 * <ul>
 *     <li>There is at most one active checkpoint advance per segment at a time. The processor does not call
 *     {@code onCheckpointAdvanced(...)} concurrently for the same segment.</li>
 *     <li>Multiple "please notify me when drained" requests may still happen before the already-queued barrier is
 *     consumed. Those requests are coalesced here onto the same queued {@link CheckpointIntent} instead of enqueueing
 *     multiple identical barrier tasks.</li>
 * </ul>
 * <p>
 * The state tracked here has the following meaning:
 * <ul>
 *     <li>{@code taskActive} is {@code true} while the workflow thread is currently executing a task obtained from the
 *     queue. A checkpoint is not safe while such a task is still running, even if the queue itself is empty.</li>
 *     <li>{@code checkpointIntentQueued} is {@code true} once a {@link CheckpointIntent} has been appended and until
 *     that intent is consumed by the workflow thread.</li>
 *     <li>{@code checkpointIntentCallback} is the callback to run after the queued barrier has been crossed. If several
 *     callers register while the same intent is pending, their callbacks are composed into one runnable.</li>
 * </ul>
 * <p>
 * The typical sequence is:
 * <ol>
 *     <li>The engine wants to know when the execution queue has drained up to the current point.</li>
 *     <li>{@link #appendCheckpointIntent(Runnable)} either reuses the already queued barrier or appends a new
 *     {@link CheckpointIntent} to the execution queue.</li>
 *     <li>The execution eventually dequeues that intent and runs it through {@link #runTask(Consumer, WorkflowExecution)}.
 *     Consuming the intent means every task that was ahead of it has completed.</li>
 *     <li>After the task finishes, the stored callback is invoked.</li>
 *     <li>The engine re-evaluates whether any work is still unsafe. If more workflow work was appended after the
 *     barrier, it can schedule another checkpoint intent and wait again.</li>
 * </ol>
 * <p>
 * Synchronization is deliberately local to this instance. The host execution already provides the single-threaded task
 * queue; this class only needs to protect its own bookkeeping about whether such a barrier task has been queued and
 * which callback should fire when it is crossed.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
final class WorkflowExecutionCheckpointSupport {

    private static final Runnable NO_OP = () -> {
    };

    /**
     * Host contract implemented by the owning workflow execution.
     * <p>
     * The support deliberately knows nothing about queue implementation details, replay state, or execution internals.
     * It only asks the host the minimal questions required to place and observe a checkpoint barrier.
     */
    interface Host {

        /**
         * Indicates whether the execution is currently processing through its live task queue.
         * <p>
         * When the execution is not executable yet, there is no concurrent queue-drain problem to coordinate with, so
         * checkpoint callbacks can be completed immediately.
         *
         * @return {@code true} if queue-based execution is active, otherwise {@code false}
         */
        boolean isExecutable();

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
         * {@link WorkflowExecutionCheckpointSupport} uses this to inject {@link CheckpointIntent} barrier tasks into the exact same
         * queue that carries normal workflow work, which is what makes the barrier meaningful.
         *
         * @param task the task to append
         */
        void appendTask(@Nonnull Consumer<WorkflowExecution> task);
    }

    private final Host host;
    private final AtomicBoolean taskActive = new AtomicBoolean(false);
    private boolean checkpointIntentQueued;
    private Runnable checkpointIntentCallback = NO_OP;

    /**
     * Creates checkpoint support bound to a single workflow execution host.
     *
     * @param host the execution facade used to inspect queue state and append barrier tasks
     */
    @Internal
    WorkflowExecutionCheckpointSupport(@Nonnull Host host) {
        this.host = Objects.requireNonNull(host, "Checkpoint support host must not be null");
    }

    /**
     * Requests notification once the execution queue has crossed the current checkpoint barrier.
     * <p>
     * If the execution is not live yet, the callback is invoked immediately because no asynchronous task queue is
     * active. Otherwise this method ensures that exactly one {@link CheckpointIntent} is queued for the current drain
     * cycle.
     * <p>
     * Repeated calls while that same intent is still pending do not enqueue more tasks. Instead, their callbacks are
     * composed into {@link #checkpointIntentCallback} so they all fire when the single queued barrier is consumed.
     * <p>
     * The barrier task is appended outside the synchronized block. The synchronized section only decides whether a new
     * barrier is needed; once that decision is made, queueing the task does not need to hold this object's monitor.
     *
     * @param onDrained callback invoked after the queued checkpoint barrier has been consumed
     */
    void appendCheckpointIntent(@Nonnull Runnable onDrained) {
        var callback = Objects.requireNonNull(onDrained, "On drained callback must not be null");
        if (!host.isExecutable()) {
            callback.run();
            return;
        }

        synchronized (this) {
            checkpointIntentCallback = checkpointIntentCallback == NO_OP
                    ? callback
                    : compose(checkpointIntentCallback, callback);
            if (checkpointIntentQueued) {
                return;
            }
            checkpointIntentQueued = true;
        }
        host.appendTask(new CheckpointIntent());
    }

    /**
     * Determines whether a checkpoint would still be unsafe for this execution.
     * <p>
     * A checkpoint remains unsafe while any of the following is true:
     * <ul>
     *     <li>a workflow task is currently executing,</li>
     *     <li>ordinary workflow tasks remain queued, or</li>
     *     <li>a {@link CheckpointIntent} barrier has been queued but not yet consumed.</li>
     * </ul>
     * <p>
     * When the execution is not live, the answer is always {@code false} because there is no asynchronous queue work
     * to wait for.
     *
     * @return {@code true} if checkpoint advancement must still wait, otherwise {@code false}
     */
    boolean hasPendingCheckpointWork() {
        if (!host.isExecutable()) {
            return false;
        }
        synchronized (this) {
            return taskActive.get() || host.hasQueuedTasks() || checkpointIntentQueued;
        }
    }

    /**
     * Indicates whether the given queued task is a checkpoint barrier owned by this support instance.
     *
     * @param task queued workflow task to inspect
     * @return {@code true} if the task is a checkpoint barrier
     */
    boolean isCheckpointIntent(@Nonnull Consumer<WorkflowExecution> task) {
        return task instanceof CheckpointIntent;
    }

    /**
     * Completes any checkpoint callbacks whose barriers cannot be consumed because the workflow driver has stopped.
     * <p>
     * Once the driver is no longer executable, no further queue work can make checkpoint advancement unsafe. Releasing
     * the callbacks prevents a checkpoint waiter from being stranded by terminal cleanup.
     */
    void completePendingCheckpointIntent() {
        Runnable callback;
        synchronized (this) {
            checkpointIntentQueued = false;
            callback = checkpointIntentCallback;
            checkpointIntentCallback = NO_OP;
        }
        callback.run();
    }

    /**
     * Executes one queue task while maintaining checkpoint bookkeeping.
     * <p>
     * This method marks a task as active for the duration of execution so
     * {@link #hasPendingCheckpointWork()} can observe that the queue is not yet safe even when it appears empty.
     * <p>
     * When the task is a {@link CheckpointIntent}, consuming it snapshots the currently composed callback. That callback
     * is then invoked after the task finishes and after {@code taskActive} has been cleared, so the subsequent safety
     * re-check sees the most up-to-date state.
     *
     * @param task the queue task to run
     * @param execution the execution instance passed to the task
     */
    void runTask(@Nonnull Consumer<WorkflowExecution> task,
                 @Nonnull WorkflowExecution execution) {
        Runnable afterTask;
        taskActive.set(true);
        try {
            task.accept(execution);
            afterTask = task instanceof CheckpointIntent checkpointIntent ? checkpointIntent.onDrained() : null;
        } finally {
            taskActive.set(false);
        }
        if (afterTask != null) {
            afterTask.run();
        }
    }

    /**
     * Synthetic queue task that acts as the checkpoint barrier marker.
     * <p>
     * The task itself does not mutate workflow state. Its purpose is positional: once it reaches the head of the
     * queue, every task that was queued before the checkpoint request has already run. At that moment it captures and
     * clears the currently accumulated drain callback so that the callback can be invoked after the task completes.
     */
    private final class CheckpointIntent implements Consumer<WorkflowExecution> {

        private Runnable onDrained = NO_OP;

        /**
         * Marks the queued checkpoint barrier as consumed and captures the callback associated with that barrier.
         * <p>
         * This runs on the workflow thread as part of normal queue consumption. Clearing
         * {@link #checkpointIntentQueued} here allows a later checkpoint cycle to enqueue a fresh barrier if new work
         * appears after this point.
         *
         * @param ignored the workflow execution, unused because this task is a pure barrier marker
         */
        @Override
        public void accept(WorkflowExecution ignored) {
            synchronized (WorkflowExecutionCheckpointSupport.this) {
                checkpointIntentQueued = false;
                onDrained = checkpointIntentCallback;
                checkpointIntentCallback = NO_OP;
            }
        }

        /**
         * Returns the callback captured when this barrier task was consumed.
         *
         * @return the callback to run after the barrier task has finished
         */
        private Runnable onDrained() {
            return onDrained;
        }
    }

    /**
     * Composes two callbacks into one, preserving registration order.
     *
     * @param first the callback to run first
     * @param second the callback to run second
     * @return a runnable that executes both callbacks in sequence
     */
    private static Runnable compose(@Nonnull Runnable first,
                                    @Nonnull Runnable second) {
        return () -> {
            first.run();
            second.run();
        };
    }
}
