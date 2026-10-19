/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowCancelledException;
import io.axoniq.framework.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.util.WorkflowStateUtils;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.CancellationException;
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
 * @since 5.4.0
 */
final class DefaultWorkflowCancellation implements WorkflowCancellation.Request {

    private final WorkflowExecution workflowExecution;
    private final WorkflowExecutionOperationsDelegation workflowExecutionOperations;
    private final RunningSteps runningSteps;
    private final AtomicReference<WorkflowCancellation.PendingRequest> pendingWorkflowCancellation = new AtomicReference<>();
    private final AtomicBoolean workflowCancellationConsumed = new AtomicBoolean();
    private final Object workflowCancellationMonitor = new Object();

    /**
     * Creates a cancellation coordinator for one workflow execution.
     *
     * @param workflowExecution           workflow execution providing control-thread mechanics
     * @param workflowExecutionOperations runtime operations delegating terminal primitive operations
     * @param runningSteps                registry of active asynchronous step executions
     */
    DefaultWorkflowCancellation(WorkflowExecution workflowExecution,
                                WorkflowExecutionOperationsDelegation workflowExecutionOperations,
                                RunningSteps runningSteps) {
        this.workflowExecution = Objects.requireNonNull(workflowExecution, "Workflow execution is mandatory");
        this.workflowExecutionOperations = Objects.requireNonNull(workflowExecutionOperations,
                                                                  "Workflow execution operations are mandatory");
        this.runningSteps = Objects.requireNonNull(runningSteps, "Running steps are mandatory");
    }

    @Override
    public CompletableFuture<Boolean> requestStepCancellation(String stepName, @Nullable Throwable cause) {
        if (!WorkflowStateUtils.isStepActive(workflowExecution.state(), stepName)) {
            return CompletableFuture.completedFuture(false);
        }
        var done = new CompletableFuture<Boolean>();
        workflowExecution.appendTask(ignored -> {
            try {
                done.complete(workflowExecutionOperations.cancelStep(PrimitiveCommands.cancelStep(
                        stepName, cause, workflowExecution.workflowConfiguration().eventNameCustomizer()
                                                          .forStepInheritance())));
            } catch (Throwable t) {
                done.completeExceptionally(t);
            }
        });
        return done;
    }

    @Override
    public CompletableFuture<Integer> requestCancellationOfAllSteps(@Nullable Throwable cause) {
        var stepNames = runningSteps.stepNames();
        if (stepNames.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        var done = new CompletableFuture<Integer>();
        workflowExecution.appendTask(ignored -> {
            try {
                var cancelled = 0;
                for (var stepName : stepNames) {
                    if (workflowExecutionOperations.cancelStep(PrimitiveCommands.cancelStep(
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

    @Override
    public CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause) {
        var cancellationCause = cancellationCause(cause);
        var request = new WorkflowCancellation.PendingRequest(cancellationCause, new CompletableFuture<>());
        synchronized (workflowCancellationMonitor) {
            var pending = pendingWorkflowCancellation.get();
            if (pending != null) {
                return pending.callback();
            }
            pendingWorkflowCancellation.set(request);
        }
        try {
            workflowExecution.interruptWorkflowDriver();
        } catch (Throwable error) {
            abortPendingWorkflowCancellation(new CancellationException(
                    "Workflow cancellation could not wake the workflow driver"));
            request.callback().completeExceptionally(error);
        }
        return request.callback();
    }

    @Override
    public boolean hasPendingWorkflowCancellation() {
        synchronized (workflowCancellationMonitor) {
            return pendingWorkflowCancellation.get() != null && !workflowCancellationConsumed.get();
        }
    }

    @Override
    public WorkflowCancellation.@Nullable PendingRequest consumeWorkflowCancellation() {
        synchronized (workflowCancellationMonitor) {
            var cancellation = pendingWorkflowCancellation.get();
            return cancellation != null && workflowCancellationConsumed.compareAndSet(false, true)
                    ? cancellation
                    : null;
        }
    }

    @Override
    public void abortPendingWorkflowCancellation(CancellationException reason) {
        Objects.requireNonNull(reason, "Cancellation reason is mandatory");
        WorkflowCancellation.PendingRequest pending;
        synchronized (workflowCancellationMonitor) {
            pending = pendingWorkflowCancellation.get();
            if (pending == null || !workflowCancellationConsumed.compareAndSet(false, true)) {
                return;
            }
        }
        pending.callback().completeExceptionally(reason);
    }

    private WorkflowCancelledException cancellationCause(@Nullable Throwable cause) {
        if (cause instanceof WorkflowCancelledException cancelled) {
            return cancelled;
        }
        return cause == null
                ? new WorkflowCancelledException("Workflow cancelled externally")
                : new WorkflowCancelledException("Workflow cancelled externally", cause);
    }
}
