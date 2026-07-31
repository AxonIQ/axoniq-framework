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

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Represents the mechanics of a workflow execution accessed by the workflow engine and its primitives.
 * <p>
 * This contract deliberately excludes workflow and step cancellation policy. Cancellation is coordinated separately so
 * an execution remains focused on its control queue, state, event delivery, and local lifecycle.
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
     * Executes the workflow body.
     *
     * @param terminationHandler handler invoked after the execution reaches a terminal {@link WorkflowStatus}
     */
    void execute(@Nonnull Consumer<WorkflowExecution> terminationHandler);

    /**
     * Returns the workflow context of the current execution.
     *
     * @return workflow context
     */
    @Nonnull
    WorkflowContext workflowContext();

    /**
     * Applies queued tasks until the condition is satisfied.
     *
     * @param condition condition on the workflow state
     * @throws InterruptedException if interrupted while waiting
     */
    void awaitStateChange(@Nonnull Predicate<WorkflowState> condition) throws InterruptedException;

    /**
     * Delivers an event to the workflow execution.
     *
     * @param eventMessage      event message to deliver
     * @param processingContext processing context of the delivered event
     */
    void onEvent(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext);

    /**
     * Appends a task for execution by the workflow control thread.
     *
     * @param task task to execute
     */
    void appendTask(@Nonnull Consumer<WorkflowExecution> task);

    /**
     * Interrupts the workflow driver so it can re-evaluate pending external control requests.
     * <p>
     * This is a wake-up mechanism only. It does not mutate workflow state or publish workflow events.
     */
    void interruptWorkflowDriver();

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
     * Appends intent to advance the checkpoint token.
     *
     * @param onDrained runnable to execute on completion
     */
    void appendCheckpointIntent(@Nonnull Runnable onDrained);

    /**
     * Checks if the pending checkpoint work is present.
     *
     * @return {@code true} if the pending checkpoint work is present, {@code false} otherwise.
     */
    boolean hasPendingCheckpointWork();

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
    @Nonnull
    WorkflowState state();

    /**
     * Initializes this newly created execution with a workflow state.
     *
     * @param state workflow state loaded from a repository
     */
    void initializeState(@Nonnull WorkflowState state);

    /**
     * Returns the processing context of the workflow execution.
     *
     * @return processing context
     */
    @Nonnull
    ProcessingContext processingContext();

    /**
     * Returns the human-readable workflow name.
     *
     * @return workflow name
     */
    @Nonnull
    String workflowName();

    /**
     * Returns the workflow identifier.
     *
     * @return unique workflow execution identifier
     */
    @Nonnull
    String workflowId();

    /**
     * Returns the workflow configuration.
     *
     * @return workflow configuration
     */
    @Nonnull
    WorkflowConfiguration<?> workflowConfiguration();
}
