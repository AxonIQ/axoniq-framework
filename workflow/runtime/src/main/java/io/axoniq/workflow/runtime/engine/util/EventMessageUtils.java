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
package io.axoniq.workflow.runtime.engine.util;

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.time.Instant;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Utility with factory methods for workflow event messages.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class EventMessageUtils {

    private EventMessageUtils() {
        // avoid
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
        var name = customizer.getEventName(workflowName, context.getPayload(), WorkflowStatus.STARTED);
        return new GenericEventMessage(new MessageType(name), Map.of(),
                                       MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.STARTED)
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
        var name = customizer.getEventName(workflowName, context.getPayload(), WorkflowStatus.COMPLETED);
        return new GenericEventMessage(new MessageType(name), Map.of(),
                                       MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.COMPLETED)
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
        var name = customizer.getEventName(workflowName, context.getPayload(), WorkflowStatus.FAILED);
        return new GenericEventMessage(new MessageType(name), exception,
                                       MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.FAILED)
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
        var name = customizer.getEventName(workflowName, context.getPayload(), WorkflowStatus.TIMED_OUT);
        return new GenericEventMessage(new MessageType(name), time,
                                       MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.TIMED_OUT)
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
        var name = customizer.getEventName(workflowName, context.getPayload(), WorkflowStatus.CANCELLED);
        Object payload = cause != null ? cause : Map.of();
        return new GenericEventMessage(new MessageType(name), payload,
                                       MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.CANCELLED)
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
        return new GenericEventMessage(new MessageType(name), local,
                                       MetadataUtils.create(context.getWorkflowId(), stepName, StepStatus.STARTED)
        );
    }

    /**
     * Creates a new event message stating that a workflow step has completed.
     *
     * @param context    workflow context.
     * @param stepName   name of the step.
     * @param result     result of the step.
     * @param customizer event name customizer.
     * @return event message.
     */
    @Nonnull
    public static EventMessage completedStep(@Nonnull WorkflowContext context,
                                             @Nonnull String stepName,
                                             @Nonnull Map<String, Object> result,
                                             @Nonnull EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, result, StepStatus.COMPLETED);
        return new GenericEventMessage(new MessageType(name), result,
                                       MetadataUtils.create(context.getWorkflowId(), stepName, StepStatus.COMPLETED)
        );
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
        return new GenericEventMessage(new MessageType(name), exception,
                                       MetadataUtils.create(context.getWorkflowId(), stepName, StepStatus.FAILED)
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
        Object payload = cause != null ? cause : Map.of();
        return new GenericEventMessage(new MessageType(name), payload,
                                       MetadataUtils.create(context.getWorkflowId(), stepName, StepStatus.CANCELLED)
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
        return new GenericEventMessage(new MessageType(name), time,
                                       MetadataUtils.create(context.getWorkflowId(), stepName, StepStatus.TIMED_OUT)
        );
    }
}
