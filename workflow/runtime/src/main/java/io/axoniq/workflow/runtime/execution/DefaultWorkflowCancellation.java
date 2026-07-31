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

import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Default coordinator for cancellation requests against one workflow execution.
 * <p>
 * The coordinator owns cancellation policy but delegates step and workflow terminal-event production to the existing
 * primitives. It schedules all work on the execution's control thread, which is the sole consumer of its task queue.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 0.2.0
 */
final class DefaultWorkflowCancellation implements WorkflowCancellation {

    private static final long CANCELLATION_TIMEOUT_SECONDS = 5;

    private final WorkflowExecution workflowExecution;
    private final WorkflowContextDelegation workflowContext;
    private final RunningSteps runningSteps;

    /**
     * Creates a cancellation coordinator for one workflow execution.
     *
     * @param workflowExecution workflow execution providing control-thread mechanics
     * @param workflowContext workflow context delegating terminal primitive operations
     * @param runningSteps registry of active asynchronous step executions
     */
    DefaultWorkflowCancellation(@Nonnull WorkflowExecution workflowExecution,
                                @Nonnull WorkflowContextDelegation workflowContext,
                                @Nonnull RunningSteps runningSteps) {
        this.workflowExecution = Objects.requireNonNull(workflowExecution, "Workflow execution is mandatory");
        this.workflowContext = Objects.requireNonNull(workflowContext, "Workflow context is mandatory");
        this.runningSteps = Objects.requireNonNull(runningSteps, "Running steps are mandatory");
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public CompletableFuture<Boolean> cancelStep(@Nonnull String stepName, @Nullable Throwable cause) {
        if (!workflowExecution.state().containsStep(stepName)
                || workflowExecution.state().getStep(stepName).status().isTerminal()) {
            return CompletableFuture.completedFuture(false);
        }
        var done = new CompletableFuture<Boolean>();
        workflowExecution.appendTask(ignored -> {
            try {
                done.complete(workflowContext.cancelStep(PrimitiveCommands.cancelStep(
                        stepName, cause, workflowExecution.workflowConfiguration().eventNameCustomizer()
                                                               .forStepInheritance())));
            } catch (Throwable t) {
                done.completeExceptionally(t);
            }
        });
        return withTimeout(done);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public CompletableFuture<Integer> cancelRunningSteps(@Nullable Throwable cause) {
        var stepNames = runningSteps.stepNames();
        if (stepNames.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        var done = new CompletableFuture<Integer>();
        workflowExecution.appendTask(ignored -> {
            try {
                var cancelled = 0;
                for (var stepName : stepNames) {
                    if (workflowContext.cancelStep(PrimitiveCommands.cancelStep(
                            stepName, cause, workflowExecution.workflowConfiguration().eventNameCustomizer()
                                                                   .forStepInheritance()))) {
                        cancelled++;
                    }
                }
                done.complete(cancelled);
            } catch (Throwable t) {
                done.completeExceptionally(t);
            }
        });
        return withTimeout(done);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public CompletableFuture<Void> cancelWorkflow(@Nullable Throwable cause) {
        var done = new CompletableFuture<Void>();
        workflowExecution.appendTask(ignored -> {
            try {
                if (workflowExecution.state().workflowStatus().isTerminal()) {
                    done.complete(null);
                    return;
                }
                try {
                    workflowContext.cancelWorkflow(PrimitiveCommands.cancelWorkflow(
                            cancellationCause(cause), workflowExecution.workflowConfiguration().eventNameCustomizer()));
                } catch (WorkflowCancelledException expected) {
                    // The workflow-body primitive signals cancellation by throwing after the durable event is recorded.
                }
                workflowExecution.appendTask(task -> Thread.currentThread().interrupt());
            } catch (Throwable t) {
                done.completeExceptionally(t);
                return;
            }
            done.complete(null);
        });
        return withTimeout(done);
    }

    @Nonnull
    private WorkflowCancelledException cancellationCause(@Nullable Throwable cause) {
        if (cause instanceof WorkflowCancelledException cancelled) {
            return cancelled;
        }
        return cause == null
                ? new WorkflowCancelledException("Workflow cancelled externally")
                : new WorkflowCancelledException("Workflow cancelled externally", cause);
    }

    @Nonnull
    private <T> CompletableFuture<T> withTimeout(@Nonnull CompletableFuture<T> result) {
        return result.orTimeout(CANCELLATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}
