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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.TypeReference;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static io.axoniq.workflow.runtime.util.MetadataUtils.getStepName;

/**
 * Holds the current state of a workflow and modified by the {@link #evolve} method receiving messages.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class EventSourcedWorkflowState implements WorkflowState {

    private final static Logger logger = LoggerFactory.getLogger(EventSourcedWorkflowState.class);

    private final Map<String, WorkflowStep> steps = new ConcurrentHashMap<>();
    private WorkflowStatus status = WorkflowStatus.NONE;
    private Map<String, Object> payload = Map.of();
    private volatile Throwable terminationCause;

    private final WorkflowStateListenerSupport listenerSupport;

    /**
     * Creates a new workflow state without reference to a workflow context and with an empty initial payload.
     */
    public EventSourcedWorkflowState() {
        this(Map.of());
    }

    /**
     * Creates a new workflow state without reference to a workflow context.
     */
    public EventSourcedWorkflowState(
            @Nonnull Map<String, Object> payload
    ) {
        this.listenerSupport = WorkflowStateListenerSupport.EMPTY;
        this.payload = Objects.requireNonNull(payload, "Payload must be set.");
    }

    /**
     * Creates a new workflow state to be used with a given workflow context and listeners.
     *
     * @param context   workflow context to use.
     * @param listeners workflow status change listeners.
     */
    public EventSourcedWorkflowState(
            @Nonnull Map<String, Object> payload,
            @Nonnull WorkflowContext context,
            @Nonnull Map<WorkflowStatus, WorkflowStatusChangeListener> listeners
    ) {
        this.payload = Objects.requireNonNull(payload, "Payload must be set.");
        this.listenerSupport = new WorkflowStateListenerSupport(
                Objects.requireNonNull(listeners, "Workflow status listeners must be set."),
                Objects.requireNonNull(context, "Workflow context must be set.")
        );
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
    @Nonnull
    public WorkflowStatus workflowStatus() {
        return this.status;
    }

    @Override
    @Nonnull
    public List<String> workflowStepNames() {
        return steps.values()
                    .stream()
                    .sorted(Comparator.comparing(WorkflowStep::timestamp))
                    .map(WorkflowStep::stepName)
                    .toList();
    }

    @Nonnull
    public Map<String, Object> payload() {
        return Map.copyOf(payload);
    }

    @Override
    public WorkflowState evolve(
            @Nonnull EventMessage eventMessage,
            @Nonnull ProcessingContext processingContext) {
        logger.trace("Applying event {}", eventMessage.type());
        Object eventPayload = eventMessage.payloadAs(Object.class, processingContext.component(Converter.class));
        var metadata = eventMessage.metadata();
        // Apply step-level state changes — ignore transitions once already terminal
        MetadataUtils.getStepStatus(metadata).ifPresent(stepStatus -> {
            var stepName = getStepName(metadata);
            if (containsStep(stepName) && getStep(stepName).status().isTerminal()) {
                logger.debug("Ignoring step status {} for step '{}' — already in terminal state {}",
                            stepStatus, stepName, getStep(stepName).status());
                return;
            }
            switch (stepStatus) {
                case STARTED:
                    addStep(WorkflowStep.started(stepName,
                                                 eventPayload,
                                                 eventMessage.timestamp(),
                                                 processingContext));
                    break;
                case FAILED:
                    Throwable error = eventMessage.payloadAs(Throwable.class,
                                                             processingContext.component(Converter.class));
                    addStep(WorkflowStep.failed(stepName,
                                                error,
                                                eventMessage.timestamp(),
                                                processingContext));
                    break;
                case TIMED_OUT:
                    addStep(WorkflowStep.timedOut(stepName,
                                                  eventPayload,
                                                  eventMessage.timestamp(),
                                                  processingContext));
                    break;
                case COMPLETED:
                    addStep(WorkflowStep.completed(stepName,
                                                   eventPayload,
                                                   eventMessage.timestamp(),
                                                   processingContext));
                    // set payload if desired
                    evolvePayload(eventMessage, processingContext);
                    break;
                case CANCELLED:
                    addStep(WorkflowStep.cancelled(stepName,
                                                   eventMessage.timestamp(),
                                                   processingContext));
                    break;
                case RETRYING:
                    StepRetryInfo retryInfo = eventMessage.payloadAs(StepRetryInfo.class,
                                                                     processingContext.component(Converter.class));
                    addStep(WorkflowStep.retrying(stepName,
                                                  retryInfo,
                                                  eventMessage.timestamp(),
                                                  processingContext)); // TODO copy resources of the context
                    break;
                default:
                    break;
            }
        });
        // Apply workflow-level state changes — ignore transitions once already terminal
        MetadataUtils.getWorkflowStatus(metadata).
                     ifPresent(status -> {
                         if (workflowStatus().isTerminal()) {
                             logger.debug("Ignoring workflow status {} — already in terminal state {}",
                                         status, workflowStatus());
                             return;
                         }
                         final Throwable terminationCause;
                         if ((status == WorkflowStatus.FAILED || status == WorkflowStatus.CANCELLED)
                                 && eventPayload instanceof Throwable t) {
                             terminationCause = t;
                         } else {
                             terminationCause = null;
                         }
                         if (status == WorkflowStatus.STARTED) {
                             evolvePayload(eventMessage, processingContext);
                         }
                         setStatus(status, terminationCause);
                     });
        logger.trace("Finished applying event {} in thread {}", eventMessage.type(), Thread.currentThread());
        return this;
    }

    /**
     * Changes payload using named payload reducer from the metadata.
     *
     * @param eventMessage      event message contaning new payload and metadata.
     * @param processingContext processing context.
     */
    void evolvePayload(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext) {
        // set payload if desired
        MetadataUtils.payloadReducer(eventMessage.metadata()).ifPresent(reducerName -> {
            if (PayloadReducer.isDefault(reducerName)) {
                var resultReducer = PayloadReducer.byName(reducerName);
                Map<String, Object> stepPayload = eventMessage.payloadAs(new TypeReference<>() {
                }, processingContext.component(Converter.class));
                var result = resultReducer.apply(this.payload, stepPayload);
                logger.trace("Evolving payload: ({}, {}) -> {}", this.payload(), stepPayload, result);
                this.payload = result;
            }
        });
    }

    /**
     * Sets state and optional termination cause.
     *
     * @param workflowStatus   workflow status to set.
     * @param terminationCause cause of termination.
     */
    void setStatus(
            @Nonnull WorkflowStatus workflowStatus,
            @Nullable Throwable terminationCause
    ) {
        this.status = workflowStatus;
        this.terminationCause = terminationCause;
        this.listenerSupport.notify(workflowStatus);
    }

    /**
     * Adds a step to the workflow execution.
     *
     * @param workflowStep step to add.
     */
    void addStep(@Nonnull WorkflowStep workflowStep) {
        this.steps.put(workflowStep.stepName(), workflowStep);
    }

    @Override
    public void throwTerminalCause() {
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
                default -> throw new IllegalStateException("Workflow is in terminal state: " + status);
            }
        }
    }


    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        descriptor.describeProperty("status", status);
        if (terminationCause != null) {
            descriptor.describeProperty("terminationCause", terminationCause.getMessage());
        }
        descriptor.describeProperty("steps", List.copyOf(steps.keySet()));
        descriptor.describeProperty("payload", payload);
    }

    /**
     * Offload the safe usage of listeners from the state implementation and makes it safe to operate with.
     *
     * @param listeners       map of status listeners or null.
     * @param workflowContext workflow context or null.
     */
    record WorkflowStateListenerSupport(
            @Nullable Map<WorkflowStatus, WorkflowStatusChangeListener> listeners,
            @Nullable WorkflowContext workflowContext
    ) {

        /**
         * Empty state listener support, skipping any notification.
         */
        static final WorkflowStateListenerSupport EMPTY = new WorkflowStateListenerSupport(null, null);

        /**
         * Notifies listeners of a given status change.
         *
         * @param status status to notify about.
         */
        public void notify(@Nonnull WorkflowStatus status) {
            // only notify if we have a workflow context, this allows the usage without the context
            if (this.workflowContext != null && this.listeners != null) {
                var listener = this.listeners.get(status);
                if (listener != null) {
                    listener.onWorkflowStatus(status, workflowContext);
                }
            }
        }
    }
}
