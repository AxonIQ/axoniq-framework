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
     * Cancels the entire workflow — all running steps are cancelled first, then the workflow is terminated with
     * {@link WorkflowStatus#CANCELLED}.
     */
    void cancel();

    /**
     * Cancels the entire workflow with a reason — all running steps are cancelled first, then the workflow is
     * terminated with {@link WorkflowStatus#CANCELLED}.
     *
     * @param reason human-readable cancellation reason.
     */
    void cancel(@Nonnull String reason);

    /**
     * Cancels the entire workflow with a cause — all running steps are cancelled first, then the workflow is terminated
     * with {@link WorkflowStatus#CANCELLED}.
     *
     * @param cause the exception that triggered the cancellation.
     */
    void cancel(@Nonnull Throwable cause);

    /**
     * Cancel a running step.
     *
     * @param stepName name of the step.
     * @param cause    optional cause of the cancellation.
     */
    void cancelRunningStep(@Nonnull String stepName, @Nullable Throwable cause);

    /**
     * Cancel all running steps.
     *
     * @param cause optional cause of the cancellation.
     */
    void cancelAllRunningSteps(@Nullable Throwable cause);

    /**
     * Interrupt all running steps without producing any step/workflow cancellation events. Unlike
     * {@link #cancelAllRunningSteps(Throwable)}, this method is for abrupt process-level teardown (e.g. an engine
     * shutdown lifecycle hook): it completes in-flight step futures with a non-cancellation failure so the running
     * step is removed from bookkeeping and no {@code <Step>Cancelled} event is published. The workflow's state in the
     * event store is left at its most recent {@code <Step>Started} entry so the step can resume on the next app
     * start. Safe to call from any thread.
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
     * Returns the configuration of the workflow.
     *
     * @return workflow configuration.
     */
    @Nonnull
    WorkflowConfiguration<?> workflowConfiguration();
}
