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

import io.axoniq.framework.workflow.dsl.api.StepCancellationException;
import io.axoniq.framework.workflow.dsl.api.WorkflowCancelledException;
import io.axoniq.framework.workflow.dsl.api.WorkflowFailedException;
import io.axoniq.framework.workflow.runtime.api.execution.FutureResolutionTimeoutException;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowLifecycleControl;
import io.axoniq.framework.workflow.runtime.util.FutureResolver;
import io.axoniq.framework.workflow.runtime.util.WorkflowStateUtils;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.framework.workflow.runtime.util.EventMessageUtils.cancelledWorkflow;
import static io.axoniq.framework.workflow.runtime.util.EventMessageUtils.failedWorkflow;

/**
 * Delegate that applies workflow and step lifecycle-control commands on the workflow control thread.
 * <p>
 * It prepares a workflow for a terminal lifecycle event, publishes that event, and waits for the state projection. For
 * a single-step cancellation it completes the running future, leaving the owning step executor to publish the guarded
 * terminal step event.
 * <p>
 * <b>Replay-drift invariant:</b> any path that publishes a workflow- or step-level event or mutates
 * recorded state must be guarded against replay drift first. This applies to both step-level cancellation
 * ({@code cancelStep} → {@code StepStatus.CANCELLED}) and workflow-level termination ({@code ctx.fail()} /
 * {@code ctx.cancel()} → terminal workflow event), because in-flight workflows replaying older code through removed
 * steps would otherwise have a terminal event forced onto a history the new body no longer reaches.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal
public class WorkflowLifecycleControlDelegate implements WorkflowLifecycleControl {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowLifecycleControlDelegate.class);

    private final WorkflowExecutionOperations workflowExecutionOperations;
    private final WorkflowExecution workflowExecution;
    private final RunningSteps runningSteps;
    private final ReachedSteps reachedSteps;
    private final WorkflowTerminalTransition terminalTransition;
    private final String workflowName;

    /**
     * Constructs a lifecycle-control delegate.
     *
     * @param workflowExecutionOperations runtime primitive-operation surface
     * @param workflowExecution           workflow execution
     * @param runningSteps                running step registry
     * @param reachedSteps                reached steps tracker
     * @param terminalTransition          owner of workflow terminal-transition execution mechanics
     */
    @Internal
    public WorkflowLifecycleControlDelegate(
            WorkflowExecutionOperations workflowExecutionOperations,
            WorkflowExecution workflowExecution,
            RunningSteps runningSteps,
            ReachedSteps reachedSteps,
            WorkflowTerminalTransition terminalTransition
    ) {
        this.workflowExecutionOperations = Objects.requireNonNull(workflowExecutionOperations,
                                                                  "Workflow execution operations are mandatory");
        this.workflowExecution = Objects.requireNonNull(workflowExecution, "Workflow execution is mandatory");
        this.runningSteps = Objects.requireNonNull(runningSteps, "Running steps are mandatory");
        this.reachedSteps = Objects.requireNonNull(reachedSteps, "Reached steps tracker is mandatory");
        this.terminalTransition = Objects.requireNonNull(terminalTransition, "Terminal transition is mandatory");
        this.workflowName = Objects.requireNonNull(workflowExecution.workflowName(), "Workflow name is mandatory");
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void rethrowUnchecked(Throwable exception) throws T {
        throw (T) exception;
    }

    private static Exception getException(@Nullable Throwable cause) {
        return cause instanceof Exception
                ? (Exception) cause
                : cause != null ? new RuntimeException(cause) : new RuntimeException("Workflow failed");
    }

    @Override
    public void cancelWorkflow(WorkflowLifecycleControl.CancelWorkflowCommand command) {
        Objects.requireNonNull(command, "Command must not be null");
        reachedSteps.assertNoReplayDrift(
                workflowExecution.workflowId(),
                workflowExecution.state(),
                "<terminate>"
        );

        terminalTransition.transition(() -> publishCancelled(command, workflowName));

        var cause = command.cause();
        if (cause != null) {
            throw new WorkflowCancelledException(cause);
        }
        throw new WorkflowCancelledException("Workflow cancelled");
    }

    @Override
    public void failWorkflow(WorkflowLifecycleControl.FailWorkflowCommand command) {
        Objects.requireNonNull(command, "Command must not be null");
        reachedSteps.assertNoReplayDrift(
                workflowExecution.workflowId(),
                workflowExecution.state(),
                "<terminate>"
        );

        terminalTransition.transition(() -> publishFailed(command, workflowName));

        var cause = command.cause();
        var exception = getException(cause);
        throw new WorkflowFailedException(exception);
    }

    @Override
    public boolean cancelStep(WorkflowLifecycleControl.CancelStepCommand command) {
        Objects.requireNonNull(command, "Command must not be null");
        var stepName = command.stepName();
        reachedSteps.record(stepName);
        reachedSteps.assertNoReplayDrift(
                workflowExecution.workflowId(),
                workflowExecution.state(),
                stepName
        );

        if (!WorkflowStateUtils.isStepActive(workflowExecution.state(), stepName)) {
            return false;
        }

        var cause = command.cause();
        Throwable stepCause;
        if (cause instanceof StepCancellationException) {
            stepCause = cause;
        } else if (cause != null) {
            stepCause = new StepCancellationException(cause);
        } else {
            stepCause = new StepCancellationException("Step cancelled");
        }

        if (!runningSteps.cancelWithCause(stepName, stepCause)) {
            return false;
        }
        try {
            workflowExecution.awaitStateChange(WorkflowStateUtils.stepTerminal(stepName));
        } catch (InterruptedException e) {
            // The driver is interrupted when an append of this execution was rejected (another writer owns the
            // instance) or the engine shuts down. Either way no terminal step record of ours is durable.
            Thread.currentThread().interrupt();
            return false;
        }
        return WorkflowStateUtils.stepTerminal(stepName).test(workflowExecution.state());
    }

    private void publishFailed(WorkflowLifecycleControl.FailWorkflowCommand command,
                               String effectiveName) {
        var cause = command.cause();
        var eventNameCustomizer = command.eventNameCustomizer();
        var workflowDefinitionId = workflowExecution.state().workflowDefinitionId();
        var exception = getException(cause);

        logger.error("Workflow '{}' failed", workflowExecution.workflowId(), exception);

        awaitTerminalEventPublication(workflowExecution.appendWorkflowEvent(
                failedWorkflow(workflowExecutionOperations, effectiveName, exception, workflowDefinitionId,
                               eventNameCustomizer),
                workflowExecutionOperations.processingContext()), "FAILED");
    }

    private void publishCancelled(WorkflowLifecycleControl.CancelWorkflowCommand command,
                                  String effectiveName) {
        var cause = command.cause();
        var eventNameCustomizer = command.eventNameCustomizer();
        var workflowDefinitionId = workflowExecution.state().workflowDefinitionId();

        awaitTerminalEventPublication(workflowExecution.appendWorkflowEvent(
                cancelledWorkflow(workflowExecutionOperations, effectiveName, cause, workflowDefinitionId,
                                  eventNameCustomizer),
                workflowExecutionOperations.processingContext()), "CANCELLED");
    }

    /**
     * Awaits durable publication of a workflow terminal event.
     * <p>
     * Terminal transitions must not proceed before their event is durable. {@link FutureResolver} centralizes the
     * bounded waiting policy introduced for issue #280. Publication failures, including an unwrapped publication
     * exception or timeout, are logged and rethrown. A resolver timeout is normalized to
     * {@link FutureResolutionTimeoutException}, so workflow execution does not mistake it for a workflow timeout.
     *
     * @param publication    asynchronous terminal-event publication
     * @param terminalStatus terminal status represented by the event
     */
    private void awaitTerminalEventPublication(CompletableFuture<Void> publication,
                                               String terminalStatus) {
        try {
            FutureResolver.resolve(workflowExecutionOperations.processingContext(), publication);
        } catch (Throwable exception) {
            logger.error("Failed to publish {} terminal event for workflow '{}'", terminalStatus,
                         workflowExecution.workflowId(), exception);
            rethrowUnchecked(exception);
        }
    }
}
