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
package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.time.Instant;
import java.util.Map;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.util.MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD;

/**
 * Utility with factory methods for workflow event messages.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@Internal
public class EventMessageUtils {

    private EventMessageUtils() {
        // avoid
    }

    /**
     * Reads the workflow's current definition version from {@code context}, falling back to
     * {@link MessageType#DEFAULT_VERSION} when the context returns null (e.g. unit-test mocks with no stub).
     */
    private static String versionOf(@Nonnull WorkflowContext context) {
        var v = context.workflowVersion();
        return (v == null || v.isBlank()) ? MessageType.DEFAULT_VERSION : v;
    }

    /**
     * Creates a filter predicate for event messages based on the workflow ID.
     *
     * @param workflowId ID of the workflow.
     * @return predicate filtering event messages by workflow ID.
     */
    public static Predicate<EventMessage> workflowIdFilter(@Nonnull String workflowId) {
        return m -> MetadataUtils.workflowIdFilter(workflowId).test(m.metadata());
    }

    /**
     * Creates a new event message stating that a workflow has started.
     *
     * @param context      workflow context.
     * @param workflowName name of the workflow.
     * @param customizer   event name customizer.
     * @return event message.
     */
    @Nonnull
    public static EventMessage startedWorkflow(@Nonnull WorkflowContext context,
                                               @Nonnull String workflowName,
                                               @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName, context.workflowPayload(), WorkflowStatus.STARTED);
        return new GenericEventMessage(new MessageType(name, versionOf(context)), context.workflowPayload(),
                                       MetadataUtils.create(context.workflowId(), WorkflowStatus.STARTED)
                                                    .and(METADATA_KEY_MODIFY_PAYLOAD,
                                                         CombineGlobalAndLocalPayloadReducer.NAME)
        );
    }

    /**
     * Creates a new event message stating that a workflow has completed successfully.
     *
     * @param context      workflow context.
     * @param workflowName name of the workflow.
     * @param customizer   event name customizer.
     * @return event message.
     */
    @Nonnull
    public static EventMessage completedWorkflow(@Nonnull WorkflowContext context,
                                                 @Nonnull String workflowName,
                                                 @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName, context.workflowPayload(), WorkflowStatus.COMPLETED);
        return new GenericEventMessage(new MessageType(name, versionOf(context)), Map.of(),
                                       MetadataUtils.create(context.workflowId(), WorkflowStatus.COMPLETED)
        );
    }

    /**
     * Creates a new event message stating that a workflow has failed.
     *
     * @param context      workflow context.
     * @param workflowName name of the workflow.
     * @param exception    exception that caused the failure.
     * @param customizer   event name customizer.
     * @return event message.
     */
    @Nonnull
    public static EventMessage failedWorkflow(@Nonnull WorkflowContext context,
                                              @Nonnull String workflowName,
                                              @Nonnull Exception exception,
                                              @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName, context.workflowPayload(), WorkflowStatus.FAILED);
        return new GenericEventMessage(new MessageType(name, versionOf(context)), WorkflowError.from(exception),
                                       MetadataUtils.create(context.workflowId(), WorkflowStatus.FAILED)
        );
    }

    /**
     * Creates a new event message stating that a workflow has timed out.
     *
     * @param context      workflow context.
     * @param workflowName name of the workflow.
     * @param time         time when the timeout occurred.
     * @param customizer   event name customizer.
     * @return event message.
     */
    @Nonnull
    public static EventMessage timeoutWorkflow(@Nonnull WorkflowContext context,
                                               @Nonnull String workflowName,
                                               @Nonnull Instant time,
                                               @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName, context.workflowPayload(), WorkflowStatus.TIMED_OUT);
        return new GenericEventMessage(new MessageType(name, versionOf(context)), time,
                                       MetadataUtils.create(context.workflowId(), WorkflowStatus.TIMED_OUT)
        );
    }

    /**
     * Creates a new event message stating that a workflow has been cancelled.
     *
     * @param context      workflow context.
     * @param workflowName name of the workflow.
     * @param customizer   event name customizer.
     * @return event message.
     */
    @Nonnull
    public static EventMessage cancelledWorkflow(@Nonnull WorkflowContext context,
                                                 @Nonnull String workflowName,
                                                 @Nonnull EventNameCustomizer customizer) {
        return cancelledWorkflow(context, workflowName, null, customizer);
    }

    @Nonnull
    public static EventMessage cancelledWorkflow(@Nonnull WorkflowContext context,
                                                 @Nonnull String workflowName,
                                                 @Nullable Throwable cause,
                                                 @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName, context.workflowPayload(), WorkflowStatus.CANCELLED);
        return new GenericEventMessage(new MessageType(name, versionOf(context)),
                                       cause != null ? WorkflowError.from(cause) : Map.of(),
                                       MetadataUtils.create(context.workflowId(), WorkflowStatus.CANCELLED)
        );
    }

    /**
     * Creates a new event message stating that a workflow step has started.
     *
     * @param context    workflow context.
     * @param stepName   name of the step.
     * @param local      local context for the step.
     * @param customizer event name customizer.
     * @return event message.
     */
    @Nonnull
    public static EventMessage startedStep(@Nonnull WorkflowContext context,
                                           @Nonnull String stepName,
                                           @Nonnull Map<String, Object> local,
                                           @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, local, StepStatus.STARTED);
        return new GenericEventMessage(new MessageType(name, versionOf(context)), local,
                                       MetadataUtils.create(context.workflowId(), stepName, StepStatus.STARTED)
        );
    }

    /**
     * Creates a new started event for a wait-for-event step.
     *
     * @param context    workflow context
     * @param stepName   name of the step
     * @param local      local data
     * @param customizer customizer for event name
     */
    @Nonnull
    public static EventMessage startedWaitForEventStep(@Nonnull WorkflowContext context,
                                                       @Nonnull String stepName,
                                                       @Nonnull Map<String, Object> local,
                                                       @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, local, StepStatus.STARTED);
        return new GenericEventMessage(
                new MessageType(name, versionOf(context)),
                local,
                MetadataUtils.markWaitForEventStep(
                        MetadataUtils.create(context.workflowId(), stepName, StepStatus.STARTED)
                )
        );
    }

    /**
     * Creates a new event message stating that a workflow step has completed.
     *
     * @param context                  workflow context.
     * @param stepName                 name of the step.
     * @param result                   result of the step.
     * @param resultPayloadReducerName name of the result payload reducer, or {@code null} if no reducer is used, see
     *                                 {@link io.axoniq.workflow.runtime.execution.payload.LocalOnlyPayloadReducer},
     *                                 {@link io.axoniq.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer},
     *                                 {@link
     *                                 io.axoniq.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer}
     * @param customizer               event name customizer.
     * @return event message.
     */
    @Nonnull
    public static EventMessage completedStep(@Nonnull WorkflowContext context,
                                             @Nonnull String stepName,
                                             @Nonnull Map<String, Object> result,
                                             @Nullable String resultPayloadReducerName,
                                             @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, result, StepStatus.COMPLETED);
        var meta = MetadataUtils.create(context.workflowId(), stepName, StepStatus.COMPLETED);
        if (resultPayloadReducerName != null) {
            meta = meta.and(METADATA_KEY_MODIFY_PAYLOAD, resultPayloadReducerName);
        }
        return new GenericEventMessage(new MessageType(name, versionOf(context)), result, meta);
    }

    /**
     * Creates a completed event for a wait-for-event step.
     */
    @Nonnull
    public static EventMessage completedWaitForEventStep(@Nonnull WorkflowContext context,
                                                         @Nonnull String stepName,
                                                         @Nonnull Map<String, Object> result,
                                                         @Nullable String resultPayloadReducerName,
                                                         @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, result, StepStatus.COMPLETED);
        var meta = MetadataUtils.markWaitForEventStep(
                MetadataUtils.create(context.workflowId(), stepName, StepStatus.COMPLETED)
        );
        if (resultPayloadReducerName != null) {
            meta = meta.and(METADATA_KEY_MODIFY_PAYLOAD, resultPayloadReducerName);
        }
        return new GenericEventMessage(new MessageType(name, versionOf(context)), result, meta);
    }

    /**
     * Creates a new event message stating that a workflow step has failed.
     *
     * @param context    workflow context.
     * @param stepName   name of the step.
     * @param exception  exception that caused the step failure.
     * @param customizer event name customizer.
     * @return event message.
     */
    @Nonnull
    public static EventMessage failStep(@Nonnull WorkflowContext context,
                                        @Nonnull String stepName,
                                        @Nonnull Throwable exception,
                                        @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.FAILED);
        return new GenericEventMessage(new MessageType(name, versionOf(context)), WorkflowError.from(exception),
                                       MetadataUtils.create(context.workflowId(), stepName, StepStatus.FAILED)
        );
    }

    /**
     * Creates a new event message stating that a workflow step has been cancelled.
     *
     * @param context    workflow context.
     * @param stepName   name of the step.
     * @param customizer event name customizer.
     * @return event message.
     */
    @Nonnull
    public static EventMessage cancelledStep(@Nonnull WorkflowContext context,
                                             @Nonnull String stepName,
                                             @Nonnull EventNameCustomizer customizer) {
        return cancelledStep(context, stepName, null, customizer);
    }

    /**
     * Creates a new event message stating that a workflow step has been cancelled, optionally with a cause.
     *
     * @param context    workflow context.
     * @param stepName   name of the step.
     * @param cause      the cause of the cancellation, or {@code null}.
     * @param customizer event name customizer.
     * @return event message.
     */
    @Nonnull
    public static EventMessage cancelledStep(@Nonnull WorkflowContext context,
                                             @Nonnull String stepName,
                                             @Nullable Throwable cause,
                                             @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.CANCELLED);
        Object payload = cause != null ? WorkflowError.from(cause) : Map.of();
        return new GenericEventMessage(new MessageType(name, versionOf(context)), payload,
                                       MetadataUtils.create(context.workflowId(), stepName, StepStatus.CANCELLED)
        );
    }

    /**
     * Creates a cancelled event for a wait-for-event step.
     */
    @Nonnull
    public static EventMessage cancelledWaitForEventStep(@Nonnull WorkflowContext context,
                                                         @Nonnull String stepName,
                                                         @Nullable Throwable cause,
                                                         @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.CANCELLED);
        Object payload = cause != null ? WorkflowError.from(cause) : Map.of();
        return new GenericEventMessage(
                new MessageType(name, versionOf(context)),
                payload,
                MetadataUtils.markWaitForEventStep(
                        MetadataUtils.create(context.workflowId(), stepName, StepStatus.CANCELLED)
                )
        );
    }

    /**
     * Creates a new event message stating that a workflow step is retrying.
     *
     * @param context    workflow context.
     * @param stepName   name of the step.
     * @param retryInfo  retry information payload.
     * @param customizer event name customizer.
     * @return event message.
     */
    @Nonnull
    public static EventMessage retryingStep(@Nonnull WorkflowContext context,
                                            @Nonnull String stepName,
                                            @Nonnull StepRetryInfo retryInfo,
                                            @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.RETRYING);
        return new GenericEventMessage(new MessageType(name, versionOf(context)), retryInfo,
                                       MetadataUtils.create(context.workflowId(), stepName, StepStatus.RETRYING)
        );
    }

    /**
     * Creates a new event message stating that a workflow step has timed out.
     *
     * @param context    workflow context.
     * @param stepName   name of the step.
     * @param time       time when the timeout occurred.
     * @param customizer event name customizer.
     * @return event message.
     */
    @Nonnull
    public static EventMessage timeoutStep(@Nonnull WorkflowContext context,
                                           @Nonnull String stepName,
                                           @Nonnull Instant time,
                                           @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.TIMED_OUT);
        return new GenericEventMessage(new MessageType(name, versionOf(context)), time,
                                       MetadataUtils.create(context.workflowId(), stepName, StepStatus.TIMED_OUT)
        );
    }

    /**
     * Creates a timed-out event for a wait-for-event step.
     */
    @Nonnull
    public static EventMessage timeoutWaitForEventStep(@Nonnull WorkflowContext context,
                                                       @Nonnull String stepName,
                                                       @Nonnull Instant time,
                                                       @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.TIMED_OUT);
        return new GenericEventMessage(
                new MessageType(name, versionOf(context)),
                time,
                MetadataUtils.markWaitForEventStep(
                        MetadataUtils.create(context.workflowId(), stepName, StepStatus.TIMED_OUT)
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
     * @param context    workflow context.
     * @param changeId   developer-chosen identifier of the code change.
     * @param version    the new workflow version being recorded.
     * @param customizer event name customizer.
     * @return event message.
     */
    @Nonnull
    public static EventMessage versionMigrationStep(@Nonnull WorkflowContext context,
                                                    @Nonnull String changeId,
                                                    @Nonnull String version,
                                                    @Nonnull EventNameCustomizer customizer) {
        Map<String, Object> payload = Map.of(
                "changeId", changeId,
                "version", version
        );
        var name = customizer.versionMigrationEventName(changeId, payload);
        return new GenericEventMessage(new MessageType(name, version), payload,
                                       MetadataUtils.createVersionMigrationStep(context.workflowId(), changeId, version)
        );
    }
}
