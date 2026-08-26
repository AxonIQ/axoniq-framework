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

import io.axoniq.workflow.runtime.api.execution.context.Version;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.payload.PayloadReducerRegistry;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.axoniq.workflow.runtime.util.MetadataUtils.getStepName;
import static java.util.Objects.requireNonNull;

/**
 * Holds the current state of a workflow and modified by the {@link #evolve} method receiving messages.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class EventSourcedWorkflowState implements WorkflowState {

    /**
     * Type reference for a map of strings to objects used as workflow payload.
     */
    public static final TypeReference<Map<String, Object>> PAYLOAD_TYPE = new TypeReference<>() {
    };

    private final static Logger logger = LoggerFactory.getLogger(EventSourcedWorkflowState.class);

    private final String workflowId;
    private Map<String, Object> payload;
    private volatile MessageType workflowDefinition;
    private final WorkflowStateListenerSupport listenerSupport;

    private final Map<String, WorkflowStep> steps = new ConcurrentHashMap<>();
    private final Map<String, String> versions = new ConcurrentHashMap<>();
    private WorkflowStatus status = WorkflowStatus.NONE;
    private volatile Throwable terminationCause;


    /**
     * Creates a new workflow state without reference to a workflow context and with an empty initial payload.
     *
     * @param workflowId         workflow id of the workflow execution
     * @param workflowDefinition workflow definition representing the reference to the definition (name and version)
     */
    @Internal
    public EventSourcedWorkflowState(@Nonnull String workflowId,
                                     @Nonnull MessageType workflowDefinition) {
        this(workflowId, Map.of(), workflowDefinition);
    }

    /**
     * Creates a new workflow state without reference to a workflow context.
     *
     * @param workflowId         workflow id of the workflow execution
     * @param payload            initial workflow payload
     * @param workflowDefinition workflow definition representing the reference to the definition (name and version)
     */
    @Internal
    public EventSourcedWorkflowState(
            @Nonnull String workflowId,
            @Nonnull Map<String, Object> payload,
            @Nonnull MessageType workflowDefinition
    ) {
        this.workflowId = requireNonNull(workflowId, "Workflow id must be set.");
        this.payload = requireNonNull(payload, "Payload must be set.");
        this.workflowDefinition = requireNonNull(workflowDefinition, "Workflow definition id must be set.");
        this.listenerSupport = WorkflowStateListenerSupport.EMPTY;
    }

    /**
     * Creates a new workflow state seeded with the workflow definition identity.
     *
     * @param workflowId         workflow id
     * @param payload            initial workflow payload
     * @param workflowDefinition stable workflow definition identifier
     * @param context            workflow context to use
     * @param listeners          workflow status change listeners
     */
    EventSourcedWorkflowState(
            @Nonnull String workflowId,
            @Nonnull Map<String, Object> payload,
            @Nonnull MessageType workflowDefinition,
            @Nonnull WorkflowContext context,
            @Nonnull Map<WorkflowStatus, WorkflowStatusChangeListener> listeners
    ) {
        this.workflowId = requireNonNull(workflowId, "Workflow id must be set.");
        this.payload = requireNonNull(payload, "Payload must be set.");
        this.workflowDefinition = requireNonNull(workflowDefinition, "Workflow definition id must be set.");
        this.listenerSupport = new WorkflowStateListenerSupport(
                requireNonNull(listeners, "Workflow status listeners must be set."),
                requireNonNull(context, "Workflow context must be set.")
        );
    }

    EventSourcedWorkflowState(
            @Nonnull WorkflowState state,
            @Nonnull WorkflowContext context,
            @Nonnull Map<WorkflowStatus, WorkflowStatusChangeListener> listeners
    ) {
        this(requireEventSourcedState(state), context, listeners);
    }

    private static EventSourcedWorkflowState requireEventSourcedState(WorkflowState state) {
        if (!(state instanceof EventSourcedWorkflowState sourcedState)) {
            throw new IllegalArgumentException(
                    "Currently only EventSourcedWorkflowState is supported, but you passed an instance of %s.".formatted(
                            state.getClass().getName()));
        }
        return sourcedState;
    }

    /**
     * Creates a live workflow state from sourced durable state.
     *
     * @param sourcedState sourced durable workflow state
     * @param context      workflow context to use for status change notifications
     * @param listeners    workflow status change listeners
     */
    EventSourcedWorkflowState(
            @Nonnull EventSourcedWorkflowState sourcedState,
            @Nonnull WorkflowContext context,
            @Nonnull Map<WorkflowStatus, WorkflowStatusChangeListener> listeners
    ) {
        this(
                requireNonNull(sourcedState, "Sourced workflow state must not be null").workflowId,
                sourcedState.payload,
                sourcedState.workflowDefinition,
                context,
                listeners
        );
        this.steps.putAll(sourcedState.steps);
        this.versions.putAll(sourcedState.versions);
        this.status = sourcedState.status;
        this.terminationCause = sourcedState.terminationCause;
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
    public static EventCriteria criteriaBuilder(@Nonnull String workflowId) {
        return EventCriteria.havingTags(Tag.of(WorkflowEventTags.TAG_WORKFLOW_ID, workflowId));
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
        requireNonNull(changeId, "changeId must not be null");
        return versions.getOrDefault(changeId, workflowDefinition.version());
    }

    @Override
    public boolean hasVersionMigrationStep(@Nonnull String changeId) {
        return versions.containsKey(changeId);
    }

    @Override
    @Nonnull
    public String workflowDefinitionVersion() {
        return workflowDefinition.version();
    }

    @Override
    @Nonnull
    public MessageType workflowDefinitionId() {
        return workflowDefinition;
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
        return evolve(eventMessage, processingContext, true);
    }

    /**
     * Applies an event while optionally notifying workflow status listeners.
     *
     * @param eventMessage          event to apply
     * @param processingContext     context in which the event is applied
     * @param notifyStatusListeners whether a workflow status transition notifies its live listeners
     * @return this evolved workflow state
     */
    WorkflowState evolve(
            @Nonnull EventMessage eventMessage,
            @Nonnull ProcessingContext processingContext,
            boolean notifyStatusListeners) {
        logger.trace("Applying event {}", eventMessage.type());
        Object eventPayload = eventMessage.payloadAs(Object.class);
        var metadata = eventMessage.metadata();
        MetadataUtils.getWorkflowDefinitionId(metadata)
                     .ifPresent(definitionId -> this.workflowDefinition = definitionId);
        // Migration events arrive as regular COMPLETED step events that additionally carry the
        // versionChangeId + version metadata keys. They flow through the step-registration switch like
        // any other step and ALSO update the version map as a side-effect.
        if (MetadataUtils.isVersionMigrationStep(metadata)) {
            applyVersionMigrationStep(metadata);
        }
        // Apply step-level state changes — ignore transitions once already terminal
        MetadataUtils.getStepStatus(metadata).ifPresent(stepStatus -> {
            var stepName = getStepName(metadata);
            if (isStepTerminal(stepName)) {
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
                                     && !startedVersion.equals(workflowDefinition.version())) {
                                 synchronized (this) {
                                     this.workflowDefinition = new MessageType(
                                             workflowDefinition.qualifiedName(),
                                             startedVersion
                                     );
                                 }
                             }
                         }
                         setStatus(status, terminationCause, notifyStatusListeners);
                     });
        logger.trace("Finished applying event {} in thread {}", eventMessage.type(), Thread.currentThread());
        return this;
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
            if (Version.of(newVersion).isGreaterThan(Version.of(workflowDefinition.version()))) {
                workflowDefinition = new MessageType(workflowDefinition.qualifiedName(), newVersion);
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
                            stepPayload = payloadAsMap(eventMessage);
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

    private Map<String, Object> payloadAsMap(@Nonnull EventMessage eventMessage) {
        var payload = eventMessage.payloadAs(Object.class);
        if (payload instanceof Map<?, ?> map) {
            var result = new HashMap<String, Object>();
            map.forEach((key, value) -> result.put((String) key, value));
            return result;
        }
        return eventMessage.payloadAs(PAYLOAD_TYPE);
    }

    /**
     * Sets state and optional termination cause.
     *
     * @param workflowStatus        workflow status to set
     * @param terminationCause      cause of termination
     * @param notifyStatusListeners whether a workflow status transition notifies its live listeners
     */
    void setStatus(
            @Nonnull WorkflowStatus workflowStatus,
            @Nullable Throwable terminationCause,
            boolean notifyStatusListeners
    ) {
        this.status = workflowStatus;
        this.terminationCause = terminationCause;
        if (notifyStatusListeners) {
            this.listenerSupport.notify(workflowStatus);
        }
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
        descriptor.describeProperty("workflowDefinitionId", workflowDefinition);
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
                ", workflowDefinitionId=" + workflowDefinition +
                ", status=" + status +
                ", steps=" + steps +
                ", payload=" + payload +
                '}';
    }
}
