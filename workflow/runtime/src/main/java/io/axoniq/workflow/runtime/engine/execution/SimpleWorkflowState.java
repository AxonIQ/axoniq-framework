/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.api.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.engine.step.WorkflowStep;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.infra.ComponentDescriptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleWorkflowState implements WorkflowState {

    private final Map<String, WorkflowStep> steps = new ConcurrentHashMap<>();
    private WorkflowStatus status = WorkflowStatus.NONE;
    private volatile Throwable terminationCause;

    private final Map<WorkflowStatus, WorkflowStatusChangeListener> listeners;
    private final WorkflowContext context;

    public SimpleWorkflowState(
            @Nonnull WorkflowContext context,
            @Nonnull Map<WorkflowStatus, WorkflowStatusChangeListener> listeners
    ) {
        this.listeners = Objects.requireNonNull(listeners, "Workflow status listeners must be set.");
        this.context = Objects.requireNonNull(context, "Workflow context must be set.");
    }

    @Override
    public void guardTerminalState() {
        if (status.isTerminal()) {
            var cause = terminationCause;
            switch (status) {
                case FAILED -> throw cause instanceof WorkflowFailedException wfe
                        ? wfe
                        : new WorkflowFailedException(
                        cause != null ? cause : new RuntimeException("Workflow already failed"));
                case CANCELLED -> throw cause instanceof WorkflowCancelledException wce
                        ? wce
                        : new WorkflowCancelledException(
                        cause != null ? cause.getMessage() : "Workflow already cancelled");
                default -> throw new IllegalStateException(
                        "Workflow is in terminal state: " + status);
            }
        }
    }

    @Override
    @Nonnull
    public WorkflowStep getStep(@Nonnull String stepName) {
        return steps.get(stepName);
    }

    @Override
    public boolean containsStep(@Nonnull String stepName) {
        return steps.containsKey(stepName);
    }

    @Override
    public void addStep(@Nonnull WorkflowStep workflowStep) {
        this.steps.put(workflowStep.stepName(), workflowStep);
    }

    @Override
    @Nonnull
    public WorkflowStatus workflowStatus() {
        return this.status;
    }

    @Override
    @Nonnull
    public Optional<Throwable> getTerminationCause() {
        return Optional.ofNullable(this.terminationCause);
    }

    @Override
    public void setStatus(
            @Nonnull WorkflowStatus workflowStatus,
            @Nullable Throwable terminationCause
    ) {
        this.status = workflowStatus;
        this.terminationCause = terminationCause;
        // notify workflow state change listeners.
        var listener = this.listeners.get(status);
        if (listener != null) {
            listener.onWorkflowStatus(status, context);
        }
    }

    @Override
    @Nonnull
    public List<String> workflowStepNames() {
        return new ArrayList<>(steps.keySet());
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        descriptor.describeProperty("status", status);
        if (terminationCause != null) {
            descriptor.describeProperty("terminationCause", terminationCause.getMessage());
        }
        descriptor.describeProperty("steps", List.copyOf(steps.keySet()));
    }
}
