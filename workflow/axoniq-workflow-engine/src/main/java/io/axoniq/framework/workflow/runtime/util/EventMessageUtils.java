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
package io.axoniq.framework.workflow.runtime.util;

import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.dsl.api.StepRetryInfo;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowError;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowEventPublicationContext;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import org.axonframework.common.ClockUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Map;
import java.time.Clock;
import java.util.function.Predicate;

import static io.axoniq.framework.workflow.runtime.util.MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD;

/**
 * Utility with factory methods for workflow event messages.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal
public class EventMessageUtils {

    private EventMessageUtils() {
        // avoid
    }

    /**
     * Reads the workflow's current definition version from {@code workflowEventPublicationContext}, falling back to
     * {@link Version#DEFAULT_VERSION} when the operation returns null (e.g. unit-test mocks with no stub).
     */
    private static String versionOf(WorkflowEventPublicationContext workflowEventPublicationContext) {
        var v = workflowEventPublicationContext.workflowVersion();
        return (v == null || v.isBlank()) ? Version.DEFAULT_VERSION : v;
    }

    private static EventMessage eventMessage(WorkflowEventPublicationContext workflowEventPublicationContext,
                                             MessageType type,
                                             @Nullable Object payload,
                                             Metadata metadata) {
        // Timestamp from the engine's Clock component: step deadlines are compared with that clock, and a fixture
        // advances it. Both are the system clock in production. Without the component (a bare processing context in
        // a unit test) the framework's default clock applies.
        var processingContext = workflowEventPublicationContext.processingContext();
        var clock = processingContext.component(Clock.class);
        var timestamp = clock != null ? clock : ClockUtils.get();
        return new GenericEventMessage(new GenericMessage(type, payload, metadata), timestamp::instant)
                .withConverter(processingContext.component(EventConverter.class));
    }

    /**
     * Creates a filter predicate for event messages based on the workflow ID.
     *
     * @param workflowId ID of the workflow.
     * @return predicate filtering event messages by workflow ID.
     */
    public static Predicate<EventMessage> workflowIdFilter(String workflowId) {
        return m -> MetadataUtils.workflowIdFilter(workflowId).test(m.metadata());
    }

    /**
     * Creates a new event message stating that a workflow has started.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context.
     * @param workflowName                    name of the workflow.
     * @param customizer                      event name customizer.
     * @return event message.
     */
    public static EventMessage startedWorkflow(WorkflowEventPublicationContext workflowEventPublicationContext,
                                               String workflowName,
                                               VersionedType workflowDefinitionId,
                                               EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName,
                                           workflowEventPublicationContext.workflowPayload(),
                                           WorkflowStatus.STARTED);
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            workflowEventPublicationContext.workflowPayload(),
                            MetadataUtils.create(workflowEventPublicationContext.workflowId(),
                                                 WorkflowStatus.STARTED,
                                                 workflowDefinitionId)
                                         .and(METADATA_KEY_MODIFY_PAYLOAD,
                                              CombineGlobalAndLocalPayloadReducer.NAME)
        );
    }

    /**
     * Creates a new event message stating that a workflow has completed successfully.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context.
     * @param workflowName                    name of the workflow.
     * @param customizer                      event name customizer.
     * @return event message.
     */
    public static EventMessage completedWorkflow(WorkflowEventPublicationContext workflowEventPublicationContext,
                                                 String workflowName,
                                                 VersionedType workflowDefinitionId,
                                                 EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName,
                                           workflowEventPublicationContext.workflowPayload(),
                                           WorkflowStatus.COMPLETED);
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            Map.of(),
                            MetadataUtils.create(workflowEventPublicationContext.workflowId(),
                                                 WorkflowStatus.COMPLETED,
                                                 workflowDefinitionId)
        );
    }

    /**
     * Creates a new event message stating that a workflow has failed.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context
     * @param workflowName                    name of the workflow
     * @param exception                       exception that caused the failure
     * @param workflowDefinitionId            workflow definition identity
     * @param customizer                      event name customizer
     * @return event message.
     */
    public static EventMessage failedWorkflow(WorkflowEventPublicationContext workflowEventPublicationContext,
                                              String workflowName,
                                              Exception exception,
                                              VersionedType workflowDefinitionId,
                                              EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName,
                                           workflowEventPublicationContext.workflowPayload(),
                                           WorkflowStatus.FAILED);
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            WorkflowError.from(exception),
                            MetadataUtils.create(workflowEventPublicationContext.workflowId(),
                                                 WorkflowStatus.FAILED,
                                                 workflowDefinitionId)
        );
    }

    /**
     * Creates a new event message stating that a workflow has timed out.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context
     * @param workflowName                    name of the workflow
     * @param time                            time when the timeout occurred
     * @param workflowDefinitionId            workflow definition identity
     * @param customizer                      event name customizer
     * @return event message.
     */
    public static EventMessage timeoutWorkflow(WorkflowEventPublicationContext workflowEventPublicationContext,
                                               String workflowName,
                                               Instant time,
                                               VersionedType workflowDefinitionId,
                                               EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName,
                                           workflowEventPublicationContext.workflowPayload(),
                                           WorkflowStatus.TIMED_OUT);
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            time,
                            MetadataUtils.create(workflowEventPublicationContext.workflowId(),
                                                 WorkflowStatus.TIMED_OUT,
                                                 workflowDefinitionId)
        );
    }

    /**
     * Creates a new event message stating that a workflow has been cancelled.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context
     * @param workflowName                    name of the workflow
     * @param workflowDefinitionId            workflow definition identity
     * @param customizer                      event name customizer
     * @return event message.
     */
    public static EventMessage cancelledWorkflow(WorkflowEventPublicationContext workflowEventPublicationContext,
                                                 String workflowName,
                                                 VersionedType workflowDefinitionId,
                                                 EventNameCustomizer customizer) {
        return cancelledWorkflow(workflowEventPublicationContext, workflowName, null, workflowDefinitionId, customizer);
    }

    public static EventMessage cancelledWorkflow(WorkflowEventPublicationContext workflowEventPublicationContext,
                                                 String workflowName,
                                                 @Nullable Throwable cause,
                                                 VersionedType workflowDefinitionId,
                                                 EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName,
                                           workflowEventPublicationContext.workflowPayload(),
                                           WorkflowStatus.CANCELLED);
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            cause != null ? WorkflowError.from(cause) : Map.of(),
                            MetadataUtils.create(workflowEventPublicationContext.workflowId(),
                                                 WorkflowStatus.CANCELLED,
                                                 workflowDefinitionId)
        );
    }

    /**
     * Creates a new event message stating that a workflow step has started.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context.
     * @param stepName                        name of the step.
     * @param local                           local payload for the step.
     * @param customizer                      event name customizer.
     * @return event message.
     */
    public static EventMessage startedStep(WorkflowEventPublicationContext workflowEventPublicationContext,
                                           String stepName,
                                           Map<String, @Nullable Object> local,
                                           EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, local, StepStatus.STARTED);
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            local,
                            MetadataUtils.create(workflowEventPublicationContext.workflowId(),
                                                 stepName,
                                                 StepStatus.STARTED)
        );
    }

    /**
     * Creates a new started event for a wait-for-event step.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context
     * @param stepName                        name of the step
     * @param local                           local data
     * @param customizer                      customizer for event name
     */
    public static EventMessage startedWaitForEventStep(WorkflowEventPublicationContext workflowEventPublicationContext,
                                                       String stepName,
                                                       Map<String, @Nullable Object> local,
                                                       EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, local, StepStatus.STARTED);
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            local,
                            MetadataUtils.markWaitForEventStep(
                                    MetadataUtils.create(workflowEventPublicationContext.workflowId(),
                                                         stepName,
                                                         StepStatus.STARTED)
                            )
        );
    }

    /**
     * Creates a new event message stating that a workflow step has completed.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context.
     * @param stepName                        name of the step.
     * @param result                          result of the step.
     * @param resultPayloadReducerName        name of the result payload reducer, or {@code null} if no reducer is used,
     *                                        see
     *                                        {@link
     *                                        io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer},
     *                                        {@link
     *                                        io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer},
     *                                        {@link
     *                                        io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer}
     * @param customizer                      event name customizer.
     * @return event message.
     */
    public static EventMessage completedStep(WorkflowEventPublicationContext workflowEventPublicationContext,
                                             String stepName,
                                             Map<String, @Nullable Object> result,
                                             @Nullable String resultPayloadReducerName,
                                             EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, result, StepStatus.COMPLETED);
        var meta = MetadataUtils.create(workflowEventPublicationContext.workflowId(), stepName, StepStatus.COMPLETED);
        if (resultPayloadReducerName != null) {
            meta = meta.and(METADATA_KEY_MODIFY_PAYLOAD, resultPayloadReducerName);
        }
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            result,
                            meta);
    }

    /**
     * Creates a completed event for a wait-for-event step.
     */
    public static EventMessage completedWaitForEventStep(
            WorkflowEventPublicationContext workflowEventPublicationContext,
            String stepName,
            Map<String, @Nullable Object> result,
            @Nullable String resultPayloadReducerName,
            EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, result, StepStatus.COMPLETED);
        var meta = MetadataUtils.markWaitForEventStep(
                MetadataUtils.create(workflowEventPublicationContext.workflowId(), stepName, StepStatus.COMPLETED)
        );
        if (resultPayloadReducerName != null) {
            meta = meta.and(METADATA_KEY_MODIFY_PAYLOAD, resultPayloadReducerName);
        }
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            result,
                            meta);
    }

    /**
     * Creates a new event message stating that a workflow step has failed.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context.
     * @param stepName                        name of the step.
     * @param exception                       exception that caused the step failure.
     * @param customizer                      event name customizer.
     * @return event message.
     */
    public static EventMessage failStep(WorkflowEventPublicationContext workflowEventPublicationContext,
                                        String stepName,
                                        Throwable exception,
                                        EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.FAILED);
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            WorkflowError.from(exception),
                            MetadataUtils.create(workflowEventPublicationContext.workflowId(),
                                                 stepName,
                                                 StepStatus.FAILED)
        );
    }

    /**
     * Creates a new event message stating that a workflow step has been cancelled.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context.
     * @param stepName                        name of the step.
     * @param customizer                      event name customizer.
     * @return event message.
     */
    public static EventMessage cancelledStep(WorkflowEventPublicationContext workflowEventPublicationContext,
                                             String stepName,
                                             EventNameCustomizer customizer) {
        return cancelledStep(workflowEventPublicationContext, stepName, null, customizer);
    }

    /**
     * Creates a new event message stating that a workflow step has been cancelled, optionally with a cause.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context.
     * @param stepName                        name of the step.
     * @param cause                           the cause of the cancellation, or {@code null}.
     * @param customizer                      event name customizer.
     * @return event message.
     */
    public static EventMessage cancelledStep(WorkflowEventPublicationContext workflowEventPublicationContext,
                                             String stepName,
                                             @Nullable Throwable cause,
                                             EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.CANCELLED);
        Object payload = cause != null ? WorkflowError.from(cause) : Map.of();
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            payload,
                            MetadataUtils.create(workflowEventPublicationContext.workflowId(),
                                                 stepName,
                                                 StepStatus.CANCELLED)
        );
    }

    /**
     * Creates a cancelled event for a wait-for-event step.
     */
    public static EventMessage cancelledWaitForEventStep(
            WorkflowEventPublicationContext workflowEventPublicationContext,
            String stepName,
            @Nullable Throwable cause,
            EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.CANCELLED);
        Object payload = cause != null ? WorkflowError.from(cause) : Map.of();
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            payload,
                            MetadataUtils.markWaitForEventStep(
                                    MetadataUtils.create(workflowEventPublicationContext.workflowId(),
                                                         stepName,
                                                         StepStatus.CANCELLED)
                            )
        );
    }

    /**
     * Creates a new event message stating that a workflow step is retrying.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context.
     * @param stepName                        name of the step.
     * @param retryInfo                       retry information payload.
     * @param customizer                      event name customizer.
     * @return event message.
     */
    public static EventMessage retryingStep(WorkflowEventPublicationContext workflowEventPublicationContext,
                                            String stepName,
                                            StepRetryInfo retryInfo,
                                            EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.RETRYING);
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            retryInfo,
                            MetadataUtils.create(workflowEventPublicationContext.workflowId(),
                                                 stepName,
                                                 StepStatus.RETRYING)
        );
    }

    /**
     * Creates a new event message stating that a retry attempt of a workflow step has started.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context
     * @param stepName                        the name of the retry started step
     * @param retryInfo                       retry information payload, {@code attempt} is the attempt being started
     * @param customizer                      event name customizer
     * @return the retry started event message
     */
    public static EventMessage retryStartedStep(WorkflowEventPublicationContext workflowEventPublicationContext,
                                                String stepName,
                                                StepRetryInfo retryInfo,
                                                EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.RETRY_STARTED);
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            retryInfo,
                            MetadataUtils.create(workflowEventPublicationContext.workflowId(),
                                                 stepName,
                                                 StepStatus.RETRY_STARTED)
        );
    }

    /**
     * Creates a new event message stating that a workflow step has timed out.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context.
     * @param stepName                        name of the step.
     * @param time                            time when the timeout occurred.
     * @param customizer                      event name customizer.
     * @return event message.
     */
    public static EventMessage timeoutStep(WorkflowEventPublicationContext workflowEventPublicationContext,
                                           String stepName,
                                           Instant time,
                                           EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.TIMED_OUT);
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            time,
                            MetadataUtils.create(workflowEventPublicationContext.workflowId(),
                                                 stepName,
                                                 StepStatus.TIMED_OUT)
        );
    }

    /**
     * Creates a timed-out event for a wait-for-event step.
     */
    public static EventMessage timeoutWaitForEventStep(WorkflowEventPublicationContext workflowEventPublicationContext,
                                                       String stepName,
                                                       Instant time,
                                                       EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.TIMED_OUT);
        return eventMessage(workflowEventPublicationContext,
                            new MessageType(name, versionOf(workflowEventPublicationContext)),
                            time,
                            MetadataUtils.markWaitForEventStep(
                                    MetadataUtils.create(workflowEventPublicationContext.workflowId(),
                                                         stepName,
                                                         StepStatus.TIMED_OUT)
                            )
        );
    }

    /**
     * Creates a new event message recording a migration step for the given {@code changeId}. The wire-level event name
     * comes from {@link EventNameCustomizer#versionMigrationEventName(String, Map)} — by default
     * {@code <changeId>.Versioned} (e.g. {@code Payment-redesign.Versioned}). The {@code MessageType.version()} on the
     * resulting event reflects the workflow's <em>new</em> version. The event is published as a
     * {@link StepStatus#COMPLETED} step event with {@code versionChangeId} + {@code version} migration metadata keys;
     * the {@code versionChangeId} presence identifies it as a migration step.
     *
     * @param workflowEventPublicationContext read-only workflow event-publication context.
     * @param changeId                        developer-chosen identifier of the code change.
     * @param version                         the new workflow version being recorded.
     * @param customizer                      event name customizer.
     * @return event message.
     */
    public static EventMessage versionMigrationStep(WorkflowEventPublicationContext workflowEventPublicationContext,
                                                    String changeId,
                                                    String version,
                                                    EventNameCustomizer customizer) {
        Map<String, @Nullable Object> payload = Map.of(
                "changeId", changeId,
                "version", version
        );
        var name = customizer.versionMigrationEventName(changeId, payload);
        return eventMessage(workflowEventPublicationContext, new MessageType(name, version), payload,
                            MetadataUtils.createVersionMigrationStep(workflowEventPublicationContext.workflowId(),
                                                                     changeId,
                                                                     version)
        );
    }
}
