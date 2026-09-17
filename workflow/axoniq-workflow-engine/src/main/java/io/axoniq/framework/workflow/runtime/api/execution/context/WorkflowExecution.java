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
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Represents the mechanics of a workflow execution accessed by the workflow engine and its primitives.
 * <p>
 * This contract deliberately excludes workflow and step cancellation policy. Cancellation is coordinated separately so
 * an execution remains focused on its control queue, state, event delivery, and local lifecycle.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @since 5.4.0
 */
@Internal
public interface WorkflowExecution extends DescribableComponent {

    /**
     * Executes the workflow body.
     *
     * @param terminationHandler termination handler, which is executed after the execution has reached a terminal
     *                           {@link WorkflowStatus}
     * @return a future completing when the workflow body has stopped
     */
    CompletableFuture<Void> execute(Consumer<WorkflowExecution> terminationHandler);

    /**
     * Returns the runtime operations for the current execution.
     *
     * @return runtime operations for the current execution
     */
    WorkflowExecutionOperations workflowExecutionOperations();

    /**
     * Applies queued tasks until the condition is satisfied.
     *
     * @param condition condition on the workflow state
     * @throws InterruptedException if interrupted while waiting
     */
    void awaitStateChange(Predicate<WorkflowState> condition) throws InterruptedException;

    /**
     * Delivers an event to the workflow execution.
     *
     * @param eventMessage      event message to deliver
     * @param processingContext processing context of the delivered event
     */
    void onEvent(EventMessage eventMessage, ProcessingContext processingContext);

    /**
     * Appends a task for execution by the workflow control thread.
     *
     * @param task task to execute
     */
    void appendTask(Consumer<WorkflowExecution> task);

    /**
     * Interrupts the workflow driver so it can re-evaluate pending external control requests.
     * <p>
     * This is a wake-up mechanism only. It does not mutate workflow state or publish workflow events.
     */
    void interruptWorkflowDriver();

    /**
     * Appends a workflow-owned event from the given parent context.
     *
     * @param event event to append
     * @param parentContext context the append unit of work derives from
     * @return a future that completes when the event has been appended
     */
    CompletableFuture<Void> appendWorkflowEvent(EventMessage event, Context parentContext);

    /**
     * Returns and removes the next queued task.
     *
     * @return next task, or {@code null} when no task is queued
     */
    @Nullable
    Consumer<WorkflowExecution> getNextTask();

    /**
     * Returns whether the workflow execution runtime is running.
     *
     * @return true if the workflow execution runtime is running
     */
    boolean isRunning();

    /**
     * Indicates whether the workflow execution has queued tasks.
     *
     * @return {@code true} when tasks are queued
     */
    boolean hasTasks();


    /**
     * Adds a {@code latch} task to this {@code WorkflowExecution} representing a point in time when a checkpoint can be
     * made.
     *
     * @param latch the latch to run when reached in this {@code WorkflowExecution's} task queue
     */
    void addCheckpointLatch(Runnable latch);

    /**
     * Checks whether checkpoint advancement is unsafe because workflow work is active, queued, or blocked through a
     * barrier.
     *
     * @return {@code true} if checkpoint advancement is unsafe, {@code false} otherwise
     */
    boolean hasUnsafeCheckpointWork();

    /**
     * Registers a listener for transitions of checkpoint-relevant work.
     *
     * @param listener listener to notify when checkpoint work becomes safe or unsafe
     */
    void registerCheckpointWorkStateListener(CheckpointWorkStateListener listener);

    /**
     * Receives transitions of checkpoint-relevant workflow work.
     */
    interface CheckpointWorkStateListener {

        CheckpointWorkStateListener NO_OP = new CheckpointWorkStateListener() {

            @Override
            public void onMarkedUnsafe() {
                // No-op
            }

            @Override
            public void onMarkedSafe() {
                // No-op
            }
        };

        /**
         * Invoked when the workflow execution has checkpoint-relevant work.
         */
        void onMarkedUnsafe();

        /**
         * Invoked when the workflow execution no longer has checkpoint-relevant work.
         */
        void onMarkedSafe();
    }

    /**
     * Interrupt all running steps without producing any step/workflow cancellation events. This method is for abrupt
     * process-level teardown (e.g. an engine shutdown lifecycle hook): it completes in-flight step futures with a
     * non-cancellation failure so the running step is removed from bookkeeping and no {@code <Step>Cancelled} event is
     * published. The workflow's state in the event store is left at its most recent {@code <Step>Started} entry so the
     * step can resume on the next app start. Safe to call from any thread. Stops only in-memory execution as part of
     * engine shutdown.
     * <p>
     * This operation preserves the durable workflow state for replay and produces no step or workflow cancellation
     * events. It must never be used to cancel or otherwise terminate a workflow.
     */
    void stopForShutdown();

    /**
     * Returns the current workflow state.
     *
     * @return workflow state
     */
    WorkflowState state();

    /**
     * Initializes this newly created execution with a workflow state.
     *
     * @param state workflow state loaded from a repository
     */
    void initializeState(WorkflowState state);

    /**
     * Returns the processing context of the workflow execution.
     *
     * @return processing context
     */
    ProcessingContext processingContext();

    /**
     * Returns the human-readable workflow name.
     *
     * @return returns the human-readable name of the workflow
     */
    String workflowName();

    /**
     * Returns the workflow identifier.
     *
     * @return unique workflow execution identifier
     */
    String workflowId();

    /**
     * Returns the workflow configuration.
     *
     * @return workflow configuration
     */
    WorkflowConfiguration<?> workflowConfiguration();

    /**
     * Restores the position from which this execution conditions its next append.
     *
     * @param position position observed while restoring the workflow
     */
    void restoreAppendPosition(@Nullable ConsistencyMarker position);
}
