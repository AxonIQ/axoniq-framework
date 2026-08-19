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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowLifecycleControl;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.Executor;

import static io.axoniq.workflow.runtime.util.EventMessageUtils.cancelledWorkflow;
import static io.axoniq.workflow.runtime.util.EventMessageUtils.failedWorkflow;

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
 * @since 0.3.0
 */
@Internal
public class WorkflowLifecycleControlDelegate implements WorkflowLifecycleControl {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowLifecycleControlDelegate.class);

    private final WorkflowContext workflowContext;
    private final WorkflowExecution workflowExecution;
    private final RunningSteps runningSteps;
    private final ReachedSteps reachedSteps;
    private final WorkflowTerminalTransition terminalTransition;
    private final EventSink eventSink;
    private final String workflowName;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Executor executor;

    /**
     * Constructs a lifecycle-control delegate.
     *
     * @param workflowContext      workflow context.
     * @param workflowExecution    workflow execution.
     * @param runningSteps         running step registry
     * @param reachedSteps reached steps tracker
     * @param terminalTransition   owner of workflow terminal-transition execution mechanics
     * @param unitOfWorkFactory    unit of work factory for creation of new processing contexts.
     * @param eventSink            event sink for event publications.
     * @param executor             executor to offload execution tasks from workflow thread.
     */
    @Internal
    public WorkflowLifecycleControlDelegate(
            @Nonnull WorkflowContext workflowContext,
            @Nonnull WorkflowExecution workflowExecution,
            @Nonnull RunningSteps runningSteps,
            @Nonnull ReachedSteps reachedSteps,
            @Nonnull WorkflowTerminalTransition terminalTransition,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull EventSink eventSink,
            @Nonnull Executor executor
    ) {
        this.workflowContext = Objects.requireNonNull(workflowContext, "Workflow context is mandatory");
        this.workflowExecution = Objects.requireNonNull(workflowExecution, "Workflow state is mandatory");
        this.runningSteps = Objects.requireNonNull(runningSteps, "Running steps are mandatory");
        this.reachedSteps = Objects.requireNonNull(reachedSteps, "Reached steps tracker is mandatory");
        this.terminalTransition = Objects.requireNonNull(terminalTransition, "Terminal transition is mandatory");
        this.eventSink = Objects.requireNonNull(eventSink, "Event sink is mandatory");
        this.workflowName = Objects.requireNonNull(workflowExecution.workflowName(), "Workflow name is mandatory");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "UnitOfWork factory is mandatory");
        this.executor = Objects.requireNonNull(executor, "Executor is mandatory");
    }

    @Override
    public void cancelWorkflow(@Nonnull WorkflowLifecycleControl.CancelWorkflowCommand command) {
        Objects.requireNonNull(command, "Command must not be null");
        // Drift guard: adding ctx.cancel() mid-body would force a terminal event onto a
        // workflow whose old code already ran past this point. Throws non-terminally.
        reachedSteps.guardAgainstReplayDrift(workflowExecution.workflowId(),
                                                     workflowExecution.state(),
                                                     "<terminate>");

        terminalTransition.transition(() -> publishCancelled(command, workflowName));

        var cause = command.cause();
        if (cause != null) {
            throw new WorkflowCancelledException(cause);
        }
        throw new WorkflowCancelledException("Workflow cancelled");
    }

    @Override
    public void failWorkflow(@Nonnull WorkflowLifecycleControl.FailWorkflowCommand command) {
        Objects.requireNonNull(command, "Command must not be null");
        // Drift guard: adding ctx.fail() mid-body would force a terminal event onto a
        // workflow whose old code already ran past this point. Throws non-terminally.
        reachedSteps.guardAgainstReplayDrift(workflowExecution.workflowId(),
                                                     workflowExecution.state(),
                                                     "<terminate>");

        terminalTransition.transition(() -> publishFailed(command, workflowName));

        var cause = command.cause();
        var exception = getException(cause);
        throw new WorkflowFailedException(exception);
    }

    @Override
    public boolean cancelStep(@Nonnull WorkflowLifecycleControl.CancelStepCommand command) {
        Objects.requireNonNull(command, "Command must not be null");
        var stepName = command.stepName();
        reachedSteps.record(stepName);
        reachedSteps.guardAgainstReplayDrift(workflowExecution.workflowId(),
                                                     workflowExecution.state(),
                                                     stepName);

        // Guard on the single-consumer control thread: only a present, non-terminal step can be cancelled. The check
        // and the future completion below are atomic with respect to other queue tasks.
        if (!workflowExecution.state().containsStep(stepName)
                || workflowExecution.state().getStep(stepName).status().isTerminal()) {
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

        // Do NOT author <step>:CANCELLED here and do NOT touch the event sink. Complete the step's registered future
        // exceptionally; the owning step executor's completion handler then publishes <step>:CANCELLED through its
        // guarded, queue-appended sendStepEvent path (both the step-terminal and workflow-terminal guards) and runs its
        // own cleanup — exactly like every other primitive. A running execute action is not force-interrupted;
        // first-writer-wins via the terminal guard.
        if (!runningSteps.cancelWithCause(stepName, stepCause)) {
            // Non-terminal but nothing running to complete (e.g. a STARTED step with no registered future): no
            // cancellation is driven, so report false rather than block on a terminal that would never arrive.
            return false;
        }

        // Await the durable terminal record on the control thread so it cannot be lost: a result.cancel() immediately
        // followed by a whole-workflow terminal would otherwise discard the still-
        // queued CANCELLED publish. The await makes the record durable before this call returns.
        try {
            workflowExecution.awaitStateChange(s -> s.containsStep(stepName)
                    && s.getStep(stepName).status().isTerminal());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return true;
    }

    private void publishFailed(@Nonnull WorkflowLifecycleControl.FailWorkflowCommand command,
                               @Nonnull String effectiveName) {
        var cause = command.cause();
        var eventNameCustomizer = command.eventNameCustomizer();
        var workflowDefinitionId = workflowExecution.state().workflowDefinitionId();
        var exception = getException(cause);

        logger.error("Workflow '{}' failed", workflowExecution.workflowId(), exception);

        ProcessingContextUtils.executeWithResult(
                workflowExecution.workflowId(),
                unitOfWorkFactory,
                executor,
                workflowContext.processingContext(),
                ctx -> eventSink.publish(ctx,
                                         failedWorkflow(workflowContext, effectiveName, exception, workflowDefinitionId,
                                                        eventNameCustomizer))
        ).join(); // FIXME join with a timeout #280
    }

    private void publishCancelled(@Nonnull WorkflowLifecycleControl.CancelWorkflowCommand command,
                                  @Nonnull String effectiveName) {
        var cause = command.cause();
        var eventNameCustomizer = command.eventNameCustomizer();
        var workflowDefinitionId = workflowExecution.state().workflowDefinitionId();

        ProcessingContextUtils.executeWithResult(
                workflowExecution.workflowId(),
                unitOfWorkFactory,
                executor,
                workflowContext.processingContext(),
                ctx -> eventSink.publish(ctx,
                                         cancelledWorkflow(workflowContext, effectiveName, cause, workflowDefinitionId,
                                                           eventNameCustomizer))
        ).join(); // FIXME join with a timeout #280
    }

    @NonNull
    private static Exception getException(Throwable cause) {
        return cause instanceof Exception
                ? (Exception) cause
                : cause != null ? new RuntimeException(cause) : new RuntimeException("Workflow failed");
    }
}
