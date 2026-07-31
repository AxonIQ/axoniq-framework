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
package io.axoniq.workflow.runtime.api.execution.context;

import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Represents the part of the execution accessed by the Workflow Engine (internal).
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @author Allard Buijze
 * @since 1.0.0
 */
@Internal
public interface WorkflowExecution extends DescribableComponent {

    /**
     * Execute workflow.
     *
     * @param terminationHandler termination handler, which is executed after the execution has reached a terminal
     *                           {@link WorkflowStatus}.
     */
    void execute(@Nonnull Consumer<WorkflowExecution> terminationHandler);

    /**
     * Returns workflow context of the current execution.
     *
     * @return context.
     */
    WorkflowContext workflowContext();

    /**
     * Apply tasks as long the condition is not satisfied.
     *
     * @param condition condition on workflow execution.
     * @throws InterruptedException if interrupted while waiting.
     */
    void awaitStateChange(@Nonnull Predicate<WorkflowState> condition) throws InterruptedException;

    /**
     * Delivers an event to the workflow execution.
     *
     * @param eventMessage      event message.
     * @param processingContext processing context.
     */
    void onEvent(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext);

    /**
     * Append a task to the workflow execution.
     *
     * @param task a task to execute by the workflow control thread.
     */
    void appendTask(@Nonnull Consumer<WorkflowExecution> task);

    /**
     * Returns the next task to execute.
     *
     * @return new task to execute.
     */
    @Nullable
    Consumer<WorkflowExecution> getNextTask();

    /**
     * Returns true if the workflow execution is executable.
     *
     * @return true if the workflow execution is executable.
     */
    boolean isExecutable();

    /**
     * Returns true if the workflow execution has tasks to execute.
     *
     * @return true if the workflow execution has tasks to execute.
     */
    boolean hasTasks();

    /**
     * Requests cooperative cancellation of a single running step from any thread.
     * <p>
     * Mirrors {@link #requestWorkflowCancellation(Throwable)}: the cancellation is enqueued as a task onto the
     * workflow's control thread, the single consumer of the task queue, so the caller never drives workflow logic
     * directly. When the task runs it tears the step's future down; the owning step executor's completion handler
     * then records the step {@code <step>:CANCELLED} through its guarded publish path (a step already terminal is
     * skipped). The workflow itself stays alive so its body can catch the resulting
     * {@link io.axoniq.workflow.runtime.api.execution.state.StepCancellationException} and compensate.
     * <p>
     * A step unknown or already terminal at request time returns an already-completed future carrying {@code false}
     * without enqueueing anything. Otherwise the returned future completes with the outcome the control thread's
     * task actually computed once that task has fully run (the durable {@code <step>:CANCELLED} record included), or
     * completes exceptionally with a timeout if the control thread has not finished within a bound. The caller
     * decides whether to block on the result (for example via {@code join()}) or compose it asynchronously. Tasks are
     * enqueued onto a single FIFO queue with one consumer, so back-to-back requests from the same caller are still
     * processed by the control thread in the order they were made.
     *
     * @param stepName name of the step to cancel.
     * @param cause    optional cause of the cancellation, or {@code null} if none.
     * @return a future completing with {@code true} if the step was non-terminal and a durable
     * {@code <step>:CANCELLED} record was published, or {@code false} if the step was unknown or already terminal;
     * completing exceptionally if the control thread does not finish the request within its timeout.
     */
    CompletableFuture<Boolean> requestStepCancellation(@Nonnull String stepName, @Nullable Throwable cause);

    /**
     * Requests cooperative cancellation of every currently-running step of this workflow from any thread, leaving the
     * workflow itself alive.
     * <p>
     * Mirrors {@link #requestStepCancellation(String, Throwable)} applied to each running step: the work is enqueued
     * onto the control thread as one task and each non-terminal running step is recorded {@code <step>:CANCELLED}.
     * The returned future completes with the number of steps the task actually cancelled once that task has fully
     * run (every durable record included), or completes exceptionally with a timeout if the control thread has not
     * finished within a bound. The caller decides whether to block on the result or compose it asynchronously.
     * Cancellation is cooperative: each cancelled step raises a
     * {@link io.axoniq.workflow.runtime.api.execution.state.StepCancellationException} into the workflow body; an
     * uncaught exception propagates and leaves the workflow wedged non-terminal (the documented caller
     * responsibility). Tasks are enqueued onto a single FIFO queue with one consumer, so back-to-back requests from
     * the same caller are still processed by the control thread in the order they were made.
     *
     * @param cause optional cause of the cancellation, or {@code null} if none.
     * @return a future completing with the number of running steps for which a durable {@code <step>:CANCELLED}
     * record was published; completing exceptionally if the control thread does not finish the request within its
     * timeout.
     */
    CompletableFuture<Integer> requestAllRunningStepsCancellation(@Nullable Throwable cause);

    /**
     * Requests cooperative cancellation of this workflow from any thread.
     * <p>
     * The request is enqueued as a task onto the workflow's control thread, the single consumer of the task queue,
     * so the caller never drives workflow logic directly (this reuses the control-thread-safe cancellation).
     * Once the task runs it interrupts any running steps and drives the workflow to a durable {@code CANCELLED}
     * terminal state through the same path a workflow-level cancellation takes. The returned future completes once
     * that task has fully run, so the durable {@code <workflow>:CANCELLED} event is committed by the time it
     * completes, or completes exceptionally with a timeout if the control thread has not finished within a bound.
     * The caller decides whether to block on the result (for example via {@code join()}) or compose it
     * asynchronously. Tasks are enqueued onto a single FIFO queue with one consumer, so back-to-back requests from
     * the same caller are still processed by the control thread in the order they were made.
     *
     * @param cause optional cause of the cancellation, or {@code null} if none.
     * @return a future that completes once the cancellation task has fully run, or completes exceptionally if the
     * control thread does not finish the request within its timeout.
     */
    CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause);

    /**
     * Stops local execution for engine shutdown without producing step or workflow cancellation events. This method
     * completes in-flight step futures with a
     * non-cancellation failure so the running step is removed from bookkeeping and no {@code <Step>Cancelled} event is
     * published, and it unblocks the parked control thread. The workflow's state in the event store is left at its
     * most recent {@code <Step>Started} entry so the step can resume on the next app start. Safe to call from any
     * thread.
     */
    void stopForShutdown();

    /**
     * Retrieves the current state of the workflow execution.
     *
     * @return workflow state.
     */
    @Nonnull
    WorkflowState state();

    /**
     * Returns the processing context of the workflow execution.
     *
     * @return processing context.
     */
    // FIXME check if we can replace this for the Context interface
    @Nonnull
    ProcessingContext processingContext();

    /**
     * Returns the name of the workflow.
     *
     * @return returns the human-readable name of the workflow.
     */
    @Nonnull
    String workflowName();

    /**
     * Returns the id of the workflow.
     *
     * @return unique id of the workflow execution.
     */
    @Nonnull
    String workflowId();

    /**
     * Returns this execution's restart token.
     * <p>
     * This is the earliest tracking token the execution needs to restart correctly.
     *
     * @return restart token, or {@code null} if unavailable
     */
    @Nullable
    TrackingToken restartToken();

    /**
     * Returns the configuration of the workflow.
     *
     * @return workflow configuration.
     */
    @Nonnull
    WorkflowConfiguration<?> workflowConfiguration();

}
