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
import io.axoniq.workflow.runtime.util.WorkflowStateUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default coordinator for cancellation requests against one workflow execution.
 * <p>
 * The coordinator owns cancellation policy but delegates step and workflow terminal-event production to the existing
 * primitives. It schedules all work on the execution's control thread, which is the sole consumer of its task queue.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 0.3.0
 */
final class DefaultWorkflowCancellation implements WorkflowCancellation.Request {

    private final WorkflowExecution workflowExecution;
    private final WorkflowContextDelegation workflowContext;
    private final RunningSteps runningSteps;
    private final AtomicReference<WorkflowCancellation.PendingRequest> pendingWorkflowCancellation = new AtomicReference<>();
    private final AtomicBoolean workflowCancellationConsumed = new AtomicBoolean();

    /**
     * Creates a cancellation coordinator for one workflow execution.
     *
     * @param workflowExecution workflow execution providing control-thread mechanics
     * @param workflowContext   workflow context delegating terminal primitive operations
     * @param runningSteps      registry of active asynchronous step executions
     */
    DefaultWorkflowCancellation(@Nonnull WorkflowExecution workflowExecution,
                                @Nonnull WorkflowContextDelegation workflowContext,
                                @Nonnull RunningSteps runningSteps) {
        this.workflowExecution = Objects.requireNonNull(workflowExecution, "Workflow execution is mandatory");
        this.workflowContext = Objects.requireNonNull(workflowContext, "Workflow context is mandatory");
        this.runningSteps = Objects.requireNonNull(runningSteps, "Running steps are mandatory");
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> requestStepCancellation(@Nonnull String stepName, @Nullable Throwable cause) {
        if (!WorkflowStateUtils.isStepActive(workflowExecution.state(), stepName)) {
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
        return done;
    }

    @Nonnull
    @Override
    public CompletableFuture<Integer> requestRunningStepCancellations(@Nullable Throwable cause) {
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
        return done;
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause) {
        var cancellationCause = cancellationCause(cause);
        var request = new WorkflowCancellation.PendingRequest(cancellationCause, new CompletableFuture<>());
        if (pendingWorkflowCancellation.compareAndSet(null, request)) {
            workflowExecution.interruptWorkflowDriver();
            return request.completion();
        }
        return pendingWorkflowCancellation.get().completion();
    }

    @Override
    public boolean hasPendingWorkflowCancellation() {
        return pendingWorkflowCancellation.get() != null && !workflowCancellationConsumed.get();
    }

    @Nullable
    @Override
    public WorkflowCancellation.PendingRequest consumeWorkflowCancellation() {
        var cancellation = pendingWorkflowCancellation.get();
        return cancellation != null && workflowCancellationConsumed.compareAndSet(false, true)
                ? cancellation
                : null;
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

}
