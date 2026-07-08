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
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowDefinitionId;
import io.axoniq.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.payload.PayloadReducerRegistry;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import io.axoniq.workflow.runtime.util.Version;
import io.axoniq.workflow.runtime.util.WorkflowEventTagResolver;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.TypeReference;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
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

    private final String workflowId;
    private final Map<String, WorkflowStep> steps = new ConcurrentHashMap<>();
    private final Map<String, String> versions = new ConcurrentHashMap<>();
    private WorkflowStatus status = WorkflowStatus.NONE;
    private Map<String, Object> payload;
    private volatile Throwable terminationCause;
    private volatile WorkflowDefinitionId workflowDefinitionId;

    private final WorkflowStateListenerSupport listenerSupport;

    /**
     * Creates a new workflow state without reference to a workflow context and with an empty initial payload.
     */
    public EventSourcedWorkflowState(@Nonnull String workflowId,
                                     @Nonnull WorkflowDefinitionId workflowDefinitionId) {
        this(workflowId, Map.of(), workflowDefinitionId);
    }

    /**
     * Creates a new workflow state without reference to a workflow context.
     */
    public EventSourcedWorkflowState(
            @Nonnull String workflowId,
            @Nonnull Map<String, Object> payload,
            @Nonnull WorkflowDefinitionId workflowDefinitionId
    ) {
        this.workflowId = Objects.requireNonNull(workflowId, "Workflow id must be set.");
        this.listenerSupport = WorkflowStateListenerSupport.EMPTY;
        this.payload = Objects.requireNonNull(payload, "Payload must be set.");
        this.workflowDefinitionId = Objects.requireNonNull(workflowDefinitionId,
                                                           "Workflow definition id must be set.");
    }

    /**
     * Creates a new workflow state to be used with a given workflow context and listeners.
     *
     * @param context   workflow context to use.
     * @param listeners workflow status change listeners.
     */
    public EventSourcedWorkflowState(
            @Nonnull String workflowId,
            @Nonnull Map<String, Object> payload,
            @Nonnull WorkflowDefinitionId workflowDefinitionId,
            @Nonnull WorkflowContext context,
            @Nonnull Map<WorkflowStatus, WorkflowStatusChangeListener> listeners
    ) {
        this(workflowId, payload, workflowDefinitionId, context, listeners, true);
    }

    /**
     * Creates a new workflow state seeded with the workflow definition identity.
     *
     * @param workflowId           workflow id.
     * @param payload              initial workflow payload.
     * @param workflowDefinitionId stable workflow definition identifier.
     * @param context              workflow context to use.
     * @param listeners            workflow status change listeners.
     */
    private EventSourcedWorkflowState(
            @Nonnull String workflowId,
            @Nonnull Map<String, Object> payload,
            @Nonnull WorkflowDefinitionId workflowDefinitionId,
            @Nonnull WorkflowContext context,
            @Nonnull Map<WorkflowStatus, WorkflowStatusChangeListener> listeners,
            boolean ignored
    ) {
        this.workflowId = Objects.requireNonNull(workflowId, "Workflow id must be set.");
        this.payload = Objects.requireNonNull(payload, "Payload must be set.");
        this.workflowDefinitionId = Objects.requireNonNull(workflowDefinitionId,
                                                           "Workflow definition id must be set.");
        this.listenerSupport = new WorkflowStateListenerSupport(
                Objects.requireNonNull(listeners, "Workflow status listeners must be set."),
                Objects.requireNonNull(context, "Workflow context must be set.")
        );
    }

    @Override
    @Nonnull
    public String workflowId() {
        return workflowId;
    }

    /**
     * Criteria selecting all workflow-owned events for a single workflow identifier.
     *
     * @param workflowId workflow identifier
     * @return event criteria for workflow-state reconstruction
     */
    @Nonnull
    public static EventCriteria workflowEvents(@Nonnull String workflowId) {
        return EventCriteria.havingTags(Tag.of(WorkflowEventTagResolver.TAG_WORKFLOW_ID, workflowId));
    }

    @Override
    public WorkflowStep getStep(@Nonnull String stepName) {
        return steps.get(stepName);
    }

    @Override
    public boolean containsStep(@Nonnull String stepName) {
        return steps.containsKey(stepName);
    }

    @Override
    @Nonnull
    public String currentWorkflowVersion(@Nonnull String changeId) {
        Objects.requireNonNull(changeId, "changeId must not be null");
        return versions.getOrDefault(changeId, workflowDefinitionId.version());
    }

    @Override
    public boolean hasVersionMigrationStep(@Nonnull String changeId) {
        return versions.containsKey(changeId);
    }

    @Override
    @Nonnull
    public String workflowDefinitionVersion() {
        return workflowDefinitionId.version();
    }

    @Override
    @Nonnull
    public WorkflowDefinitionId workflowDefinitionId() {
        return workflowDefinitionId;
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
        Object eventPayload = eventMessage.payloadAs(Object.class);
        var metadata = eventMessage.metadata();
        MetadataUtils.getWorkflowDefinitionId(metadata)
                     .ifPresent(definitionId -> this.workflowDefinitionId = definitionId);
        // Migration events arrive as regular COMPLETED step events that additionally carry the
        // versionChangeId + version metadata keys. They flow through the step-registration switch like
        // any other step and ALSO update the version map as a side-effect.
        if (MetadataUtils.isVersionMigrationStep(metadata)) {
            applyVersionMigrationStep(metadata);
        }
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
                    Throwable stepCause;
                    try {
                        WorkflowError err = eventMessage.payloadAs(WorkflowError.class);
                        stepCause = err != null ? err.toThrowable() : null;
                    } catch (Exception e) {
                        stepCause = null;
                    }
                    addStep(WorkflowStep.failed(stepName,
                                                stepCause,
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
                    StepRetryInfo retryInfo = eventMessage.payloadAs(StepRetryInfo.class);
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
                         if (status == WorkflowStatus.FAILED || status == WorkflowStatus.CANCELLED) {
                             Throwable cause;
                             try {
                                 WorkflowError err = eventMessage.payloadAs(WorkflowError.class);
                                 cause = err != null ? err.toThrowable() : null;
                             } catch (Exception e) {
                                 // payload is not a WorkflowError
                                 cause = null;
                             }
                             terminationCause = cause;
                         } else {
                             terminationCause = null;
                         }
                         if (status == WorkflowStatus.STARTED) {
                             evolvePayload(eventMessage, processingContext);
                             var startedVersion = eventMessage.type().version();
                             if (startedVersion != null
                                     && !startedVersion.isBlank()
                                     && !startedVersion.equals(workflowDefinitionId.version())) {
                                 synchronized (this) {
                                     this.workflowDefinitionId = new WorkflowDefinitionId(
                                             workflowDefinitionId.qualifiedName(),
                                             startedVersion
                                     );
                                 }
                             }
                         }
                         setStatus(status, terminationCause);
                     });
        logger.trace("Finished applying event {} in thread {}", eventMessage.type(), Thread.currentThread());
        return this;
    }

    /**
     * Restores this live state instance from an event-sourced state loaded from the repository.
     *
     * @param restoredState sourced workflow state
     */
    void restoreFrom(@Nonnull EventSourcedWorkflowState restoredState) {
        Objects.requireNonNull(restoredState, "Restored state must not be null");
        steps.clear();
        steps.putAll(restoredState.steps);
        versions.clear();
        versions.putAll(restoredState.versions);
        status = restoredState.status;
        payload = restoredState.payload;
        terminationCause = restoredState.terminationCause;
        if (!workflowId.equals(restoredState.workflowId)) {
            throw new IllegalArgumentException(
                    "Cannot restore workflow state for '%s' into '%s'."
                            .formatted(restoredState.workflowId, workflowId)
            );
        }
        workflowDefinitionId = restoredState.workflowDefinitionId;
    }

    /**
     * Migration steps: first writer wins (replay-idempotent); bump workflow version if strictly greater.
     */
    private void applyVersionMigrationStep(@Nonnull Metadata metadata) {
        var changeId = MetadataUtils.getVersionChangeId(metadata).orElse(null);
        var newVersion = MetadataUtils.getVersion(metadata).orElse(null);
        if (changeId == null || newVersion == null) {
            return;
        }
        versions.putIfAbsent(changeId, newVersion);
        synchronized (this) {
            if (Version.of(newVersion).isGreaterThan(Version.of(workflowDefinitionId.version()))) {
                workflowDefinitionId = new WorkflowDefinitionId(workflowDefinitionId.qualifiedName(), newVersion);
            }
        }
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
            processingContext
                    .component(PayloadReducerRegistry.class)
                    .get(reducerName)
                    .ifPresentOrElse(resultReducer -> {
                        Map<String, Object> stepPayload;
                        try {
                            stepPayload = eventMessage.payloadAs(new TypeReference<>() {
                            });
                        } catch (Exception e) {
                            logger.debug(
                                    "Could not convert payload to map for reducer {}. Skipping payload update.",
                                    reducerName,
                                    e);
                            stepPayload = null;
                        }
                        if (stepPayload != null) {
                            var result = resultReducer.apply(this.payload, stepPayload);
                            logger.trace("Evolving payload: ({}, {}) -> {}",
                                         this.payload(),
                                         stepPayload,
                                         result);
                            this.payload = result;
                        }
                    }, () -> logger.warn("Unknown reducer detected {}. Skipping payload update.", reducerName));
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
        descriptor.describeProperty("versions", Map.copyOf(versions));
        descriptor.describeProperty("workflowId", workflowId);
        descriptor.describeProperty("workflowDefinitionId", workflowDefinitionId);
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

    public String toString() {
        return "WorkflowState{" +
                "workflowId='" + workflowId + '\'' +
                ", workflowDefinitionId=" + workflowDefinitionId +
                ", status=" + status +
                ", steps=" + steps +
                ", payload=" + payload +
                '}';
    }
}
