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

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.TerminatePrimitive;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.Executor;

import static io.axoniq.workflow.runtime.util.EventMessageUtils.cancelledWorkflow;
import static io.axoniq.workflow.runtime.util.EventMessageUtils.failedWorkflow;

/**
 * Delegate that owns the full workflow termination flow: cancel futures, send event, apply state, throw.
 * <p>
 * <b>Replay-drift invariant:</b> any path that publishes a workflow- or step-level event or mutates
 * recorded state must call {@link WorkflowExecution#guardAgainstReplayDrift(String)} first. This applies
 * to both step-level cancellation ({@code cancelStep} → {@code StepStatus.CANCELLED}) and
 * workflow-level termination ({@code ctx.fail()} / {@code ctx.cancel()} → terminal workflow event),
 * because in-flight workflows replaying older code through removed steps would otherwise have a terminal
 * event forced onto a history the new body no longer reaches.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@Internal
public class TerminateDelegate implements TerminatePrimitive {

    private static final Logger logger = LoggerFactory.getLogger(TerminateDelegate.class);

    private final WorkflowContext workflowContext;
    private final WorkflowExecution workflowExecution;
    private final RunningSteps runningSteps;
    private final Runnable terminalTeardown;
    private final EventSink eventSink;
    private final String workflowName;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Executor executor;
    private final EventNameCustomizer stepParentEventNameCustomizer;

    /**
     * Constructs the terminate delegate.
     *
     * @param workflowContext               workflow context.
     * @param workflowExecution             workflow execution.
     * @param runningSteps                  running step registry
     * @param terminalTeardown              operation preparing the execution for a terminal workflow event
     * @param unitOfWorkFactory             unit of work factory for creation of new processing contexts.
     * @param eventSink                     event sink for event publications.
     * @param executor                      executor to offload execution tasks from workflow thread.
     * @param stepParentEventNameCustomizer the step-inherited event name customizer, merged with the per-step
     *                                      customizer when authoring a {@code <step>:CANCELLED} record.
     */
    @Internal
    public TerminateDelegate(
            @Nonnull WorkflowContext workflowContext,
            @Nonnull WorkflowExecution workflowExecution,
            @Nonnull RunningSteps runningSteps,
            @Nonnull Runnable terminalTeardown,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull EventSink eventSink,
            @Nonnull Executor executor,
            @Nonnull EventNameCustomizer stepParentEventNameCustomizer
    ) {
        this.workflowContext = Objects.requireNonNull(workflowContext, "Workflow context is mandatory");
        this.workflowExecution = Objects.requireNonNull(workflowExecution, "Workflow state is mandatory");
        this.runningSteps = Objects.requireNonNull(runningSteps, "Running steps are mandatory");
        this.terminalTeardown = Objects.requireNonNull(terminalTeardown, "Terminal teardown is mandatory");
        this.eventSink = Objects.requireNonNull(eventSink, "Event sink is mandatory");
        this.workflowName = Objects.requireNonNull(workflowExecution.workflowName(), "Workflow name is mandatory");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "UnitOfWork factory is mandatory");
        this.executor = Objects.requireNonNull(executor, "Executor is mandatory");
        this.stepParentEventNameCustomizer = Objects.requireNonNull(stepParentEventNameCustomizer,
                                                                    "Step parent event name customizer is mandatory");
    }

    @Override
    public void cancelWorkflow(@Nonnull CancelWorkflow command) {
        // Drift guard: adding ctx.cancel() mid-body would force a terminal event onto a
        // workflow whose old code already ran past this point. Throws non-terminally.
        workflowExecution.guardAgainstReplayDrift("<terminate>");

        var effectiveName = command.workflowNameOverride() != null
                ? command.workflowNameOverride()
                : workflowName;

        // Whole-workflow terminal: interrupt running steps (no per-step event) and discard the queue
        // before publishing, so the awaitStateChange inside cancelled(...) pumps only the workflow-terminal evolve
        // and no queued retry-failure/launch task runs. Running steps stay in their last recorded state.
        terminalTeardown.run();

        cancelled(command, effectiveName);
    }

    @Override
    public void failWorkflow(@Nonnull FailWorkflow command) {
        // Drift guard: adding ctx.fail() mid-body would force a terminal event onto a
        // workflow whose old code already ran past this point. Throws non-terminally.
        workflowExecution.guardAgainstReplayDrift("<terminate>");

        var effectiveName = command.workflowNameOverride() != null
                ? command.workflowNameOverride()
                : workflowName;

        // Whole-workflow terminal: interrupt running steps (no per-step event) and discard the queue
        // before publishing, so the awaitStateChange inside failed(...) pumps only the workflow-terminal evolve and
        // no queued retry-failure/launch task runs. Running steps stay in their last recorded state.
        terminalTeardown.run();

        failed(command, effectiveName);
    }

    @Override
    public boolean cancelStep(@Nonnull CancelStep command) {
        var stepName = command.stepName();
        workflowExecution.recordStepReference(stepName);
        workflowExecution.guardAgainstReplayDrift(stepName);

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

    protected void failed(@Nonnull FailWorkflow command, @Nonnull String effectiveName) {
        var cause = command.cause();
        var eventNameCustomizer = command.eventNameCustomizer();
        var exception = cause instanceof Exception
                ? (Exception) cause
                : cause != null ? new RuntimeException(cause) : new RuntimeException("Workflow failed");

        logger.error("Workflow '{}' failed", workflowExecution.workflowId(), exception);

        ProcessingContextUtils.executeWithResult(
                workflowExecution.workflowId(),
                unitOfWorkFactory,
                executor,
                workflowContext.processingContext(),
                ctx -> eventSink.publish(ctx,
                                         failedWorkflow(workflowContext, effectiveName, exception, eventNameCustomizer))
        ).join(); // FIXME join

        try {
            workflowExecution.awaitStateChange(s -> s.workflowStatus() == WorkflowStatus.FAILED);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        throw new WorkflowFailedException(exception);
    }

    protected void cancelled(@Nonnull CancelWorkflow command, @Nonnull String effectiveName) {
        var cause = command.cause();
        var eventNameCustomizer = command.eventNameCustomizer();

        ProcessingContextUtils.executeWithResult(
                workflowExecution.workflowId(),
                unitOfWorkFactory,
                executor,
                workflowContext.processingContext(),
                ctx -> eventSink.publish(ctx,
                                         cancelledWorkflow(workflowContext, effectiveName, cause, eventNameCustomizer))
        ).join(); // FIXME join

        try {
            workflowExecution.awaitStateChange(s -> s.workflowStatus() == WorkflowStatus.CANCELLED);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (cause != null) {
            throw new WorkflowCancelledException(cause);
        }
        throw new WorkflowCancelledException("Workflow cancelled");
    }
}
