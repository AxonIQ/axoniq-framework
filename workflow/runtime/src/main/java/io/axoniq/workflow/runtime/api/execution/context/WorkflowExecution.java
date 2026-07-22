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
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.util.List;
import java.util.Set;
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
 * @since 0.3.0
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
     * Registers a new wait condition.
     *
     * @param stepName             waiting step name.
     * @param eventCondition       event condition.
     * @param resultPayloadReducer step result payload reducer.
     * @param eventNameCustomizer  event name customizer.
     */
    void registerWaitCondition(@Nonnull String stepName,
                               @Nonnull EventCondition eventCondition,
                               @Nonnull PayloadReducer resultPayloadReducer,
                               @Nonnull EventNameCustomizer eventNameCustomizer);

    /**
     * Remove existing wait condition.
     *
     * @param stepName name of the waiting step.
     */
    void removeWaitCondition(@Nonnull String stepName);

    /**
     * Register a running step.
     *
     * @param stepName step name.
     * @param future   future of the execution.
     */
    void registerRunningStep(@Nonnull String stepName, @Nonnull CompletableFuture<?> future);

    /**
     * Remove a running step.
     *
     * @param stepName step name.
     */
    void removeRunningStep(@Nonnull String stepName);

    /**
     * Cancels a running step by completing its registered future exceptionally with the given cause. The owning step
     * executor's completion handler then publishes the {@code <step>:CANCELLED} record through its guarded publish path
     * and runs its own cleanup; nothing is published directly here.
     *
     * @param stepName name of the step.
     * @param cause    optional cause of the cancellation, or {@code null} if none.
     * @return {@code true} if a running future was registered for the step and was completed by this call;
     * {@code false} if no running future was registered.
     */
    boolean cancelRunningStep(@Nonnull String stepName, @Nullable Throwable cause);

    /**
     * Cooperatively cancels a single running step from the workflow's own control thread (the in-body twin of
     * {@link #requestStepCancellation(String, Throwable)}).
     * <p>
     * Unlike {@link #requestStepCancellation(String, Throwable)}, this completes the step's future and then
     * <b>awaits</b> the durable {@code <step>:CANCELLED} record (the caller is already the control thread), so the
     * record is durable before the caller proceeds — it cannot be lost to a subsequent whole-workflow terminal
     * discarding the queue. The record itself is published by the owning step executor's completion handler, not
     * authored here. Used by {@code WorkflowStepResult.cancel()}.
     *
     * @param stepName name of the step to cancel.
     * @param cause    optional cause of the cancellation, or {@code null} if none.
     * @return {@code true} if the step was non-terminal and a {@code <step>:CANCELLED} record was published;
     * {@code false} if the step was unknown or already terminal.
     */
    boolean cancelStep(@Nonnull String stepName, @Nullable Throwable cause);

    /**
     * Requests cooperative cancellation of a single running step from any thread.
     * <p>
     * Mirrors {@link #requestWorkflowCancellation(Throwable)}: the cancellation is enqueued as a task onto the
     * workflow's control thread — the single consumer of the task queue — so the caller never drives workflow logic
     * directly. When the task runs it tears the step's future down; the owning step executor's completion handler
     * then records the step {@code <step>:CANCELLED} through its guarded publish path (a step already terminal is
     * skipped). The workflow itself stays alive so its body can catch the resulting
     * {@link io.axoniq.workflow.runtime.api.execution.state.StepCancellationException} and compensate.
     * <p>
     * The returned boolean reflects the step's status at request time: {@code true} when the step exists and is
     * non-terminal (a cancellation was enqueued), {@code false} when the step is unknown or already terminal (nothing
     * enqueued).
     *
     * @param stepName name of the step to cancel.
     * @param cause    optional cause of the cancellation, or {@code null} if none.
     * @return {@code true} if the step was non-terminal and a cancellation was enqueued; {@code false} otherwise.
     */
    boolean requestStepCancellation(@Nonnull String stepName, @Nullable Throwable cause);

    /**
     * Requests cooperative cancellation of every currently-running step of this workflow from any thread, leaving the
     * workflow itself alive.
     * <p>
     * Mirrors {@link #requestStepCancellation(String, Throwable)} applied to each running step: the work is enqueued
     * onto the control thread and each non-terminal running step is recorded {@code <step>:CANCELLED}. Cancellation is
     * cooperative — each cancelled step raises a
     * {@link io.axoniq.workflow.runtime.api.execution.state.StepCancellationException} into the workflow body; an
     * uncaught exception propagates and leaves the workflow wedged non-terminal (the documented caller
     * responsibility).
     *
     * @param cause optional cause of the cancellation, or {@code null} if none.
     * @return the number of currently-running steps for which a cancellation was enqueued.
     */
    int requestAllRunningStepsCancellation(@Nullable Throwable cause);

    /**
     * Whole-workflow terminal teardown. Interrupts every still-running step future with a
     * non-cancellation cause — so the step-completion handlers publish no per-step terminal event and merely
     * deregister — and discards every queued task so a queued retry-failure/launch task never runs. Running steps are
     * left in their last recorded state in the event log; the caller publishes the single workflow-level terminal
     * event afterwards. Must be invoked on the workflow control thread, before publishing the terminal event.
     */
    void interruptStepsAndDiscardQueue();

    /**
     * Requests cooperative cancellation of this workflow from any thread.
     * <p>
     * The request is enqueued as a task onto the workflow's control thread — the single consumer of the task queue —
     * so the caller never drives workflow logic directly (this reuses the control-thread-safe cancellation).
     * Once the task runs it cancels any running steps and drives the workflow to a durable {@code CANCELLED}
     * terminal state through the same path a workflow-level cancellation takes. It does not block on completion; the
     * durable {@code <workflow>:CANCELLED} event is committed asynchronously by the control thread.
     *
     * @param cause optional cause of the cancellation, or {@code null} if none.
     */
    void requestWorkflowCancellation(@Nullable Throwable cause);

    /**
     * Interrupt all running steps without producing any step/workflow cancellation events. Unlike
     * {@link #interruptStepsAndDiscardQueue()} (a whole-workflow terminal teardown), this method is for abrupt
     * process-level teardown (e.g. an engine shutdown lifecycle hook): it completes in-flight step futures with a
     * non-cancellation failure so the running step is removed from bookkeeping and no {@code <Step>Cancelled} event is
     * published, and it unblocks the parked control thread. The workflow's state in the event store is left at its
     * most recent {@code <Step>Started} entry so the step can resume on the next app start. Safe to call from any
     * thread.
     */
    void interrupt();

    /**
     * Cancel and remove a running step.
     *
     * @param stepName              name of the step.
     * @param mayInterruptIfRunning whether to interrupt the step if it is running.
     */
    void cancelAndRemoveRunningStep(@Nonnull String stepName, boolean mayInterruptIfRunning);

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

    /**
     * Records that the live execution has reached the step with the given name during the current
     * invocation. Forms the runtime "book"; comparing it against the event-sourced book (state) detects
     * when the code has drifted past what history accounts for.
     *
     * @param stepName step name encountered.
     */
    void recordStepReference(@Nonnull String stepName);

    /**
     * Returns the set of step names the current invocation of the workflow body has already referenced.
     *
     * @return live view of referenced step names for this invocation.
     */
    @Nonnull
    Set<String> referencedStepNames();

    /**
     * Terminal steps in {@link #state()} (event-sourced book) that the current live run has not
     * referenced (runtime book). A non-empty result means old code already ran past this position —
     * the signal used by the version primitive's downstream-steps guard and by the drift safety net.
     *
     * @return ordered list of unreferenced terminal step names; empty when state is fully accounted for.
     */
    @Nonnull
    default List<String> unreferencedTerminalSteps() {
        Set<String> referenced = referencedStepNames();
        WorkflowState state = state();
        return state.workflowStepNames().stream()
                    .filter(name -> !referenced.contains(name))
                    .filter(name -> {
                        var step = state.getStep(name);
                        return step != null && step.status().isTerminal();
                    })
                    .toList();
    }

    /**
     * Convenience boolean for {@link #unreferencedTerminalSteps()}.
     *
     * @return {@code true} iff at least one terminal step in history is not yet referenced.
     */
    default boolean hasUnreferencedTerminalStep() {
        return !unreferencedTerminalSteps().isEmpty();
    }

    /**
     * Throws {@link WorkflowReplayDriftException} when the event-sourced book contains terminal steps the
     * current run has not referenced yet — i.e. the new code is about to publish past where the old code
     * already ran.
     * <p>
     * <b>Invariant:</b> anything that publishes events or changes workflow state must call this guard
     * before doing so. Per-step primitives gate on first live publish; workflow-level termination
     * gates on the {@code "<terminate>"} marker.
     *
     * @param aboutToExecute step name about to publish.
     */
    default void guardAgainstReplayDrift(@Nonnull String aboutToExecute) {
        List<String> unreferenced = unreferencedTerminalSteps();
        if (!unreferenced.isEmpty()) {
            throw new WorkflowReplayDriftException(workflowId(), aboutToExecute, unreferenced);
        }
    }
}
