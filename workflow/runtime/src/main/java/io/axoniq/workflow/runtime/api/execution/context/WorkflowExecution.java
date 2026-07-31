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

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Represents the mechanics of a workflow execution accessed by the workflow engine and its primitives.
 * <p>
 * This contract deliberately excludes workflow and step cancellation policy. Cancellation is coordinated separately
 * so an execution remains focused on its control queue, state, event delivery, and local lifecycle.
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
     * @param eventMessage event message to deliver
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
     * Returns and removes the next queued task.
     *
     * @return next task, or {@code null} when no task is queued
     */
    @Nullable
    Consumer<WorkflowExecution> getNextTask();

    /**
     * Indicates whether the workflow execution has been started.
     *
     * @return {@code true} when the workflow execution is running
     */
    boolean isExecutable();

    /**
     * Indicates whether the workflow execution has queued tasks.
     *
     * @return {@code true} when tasks are queued
     */
    boolean hasTasks();

    /**
     * Stops local execution for engine shutdown without producing step or workflow cancellation events.
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
     * Returns the earliest tracking token required to restart this execution.
     *
     * @return restart token, or {@code null} when unavailable
     */
    @Nullable
    TrackingToken restartToken();

    /**
     * Returns the workflow configuration.
     *
     * @return workflow configuration
     */
    @Nonnull
    WorkflowConfiguration<?> workflowConfiguration();
}
