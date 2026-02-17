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
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.time.Instant;
import java.util.Map;
import java.util.function.Predicate;

public class EventMessageUtils {

    private EventMessageUtils() {
        // avoid
    }

    public static Predicate<EventMessage> workflowIdFilter(String workflowId) {
        return m -> MetadataUtils.workflowIdFilter(workflowId).test(m.metadata());
    }

    public static EventMessage startedWorkflow(WorkflowContext context, String workflowName,
                                               EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName, context.getPayload(), WorkflowStatus.STARTED);
        return new GenericEventMessage(new MessageType(name), Map.of(),
                                       MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.STARTED)
        );
    }

    public static EventMessage completedWorkflow(WorkflowContext context, String workflowName,
                                                 EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName, context.getPayload(), WorkflowStatus.COMPLETED);
        return new GenericEventMessage(new MessageType(name), Map.of(),
                                       MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.COMPLETED)
        );
    }

    public static EventMessage failedWorkflow(WorkflowContext context, String workflowName, Exception exception,
                                              EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName, context.getPayload(), WorkflowStatus.FAILED);
        return new GenericEventMessage(new MessageType(name), exception,
                                       MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.FAILED)
        );
    }

    public static EventMessage timeoutWorkflow(WorkflowContext context, String workflowName, Instant time,
                                               EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName, context.getPayload(), WorkflowStatus.TIMED_OUT);
        return new GenericEventMessage(new MessageType(name), time,
                                       MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.TIMED_OUT)
        );
    }

    public static EventMessage cancelledWorkflow(WorkflowContext context, String workflowName,
                                                 EventNameCustomizer customizer) {
        var name = customizer.getEventName(workflowName, context.getPayload(), WorkflowStatus.CANCELLED);
        return new GenericEventMessage(new MessageType(name), null,
                                       MetadataUtils.create(context.getWorkflowId(), WorkflowStatus.CANCELLED)
        );
    }

    public static EventMessage startedStep(WorkflowContext context, String stepName, Map<String, Object> local,
                                           EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, local, StepStatus.STARTED);
        return new GenericEventMessage(new MessageType(name), local,
                                       MetadataUtils.create(context.getWorkflowId(), stepName, StepStatus.STARTED)
        );
    }

    public static EventMessage completedStep(WorkflowContext context, String stepName, Map<String, Object> result,
                                             EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, result, StepStatus.COMPLETED);
        return new GenericEventMessage(new MessageType(name), result,
                                       MetadataUtils.create(context.getWorkflowId(), stepName, StepStatus.COMPLETED)
        );
    }

    public static EventMessage failStep(WorkflowContext context, String stepName, Throwable exception,
                                        EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.FAILED);
        return new GenericEventMessage(new MessageType(name), exception,
                                       MetadataUtils.create(context.getWorkflowId(), stepName, StepStatus.FAILED)
        );
    }

    public static EventMessage cancelledStep(WorkflowContext context, String stepName, EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.CANCELLED);
        return new GenericEventMessage(new MessageType(name), null,
                                       MetadataUtils.create(context.getWorkflowId(), stepName, StepStatus.CANCELLED)
        );
    }

    public static EventMessage timeoutStep(WorkflowContext context, String stepName, Instant time,
                                           EventNameCustomizer customizer) {
        var name = customizer.getEventName(stepName, Map.of(), StepStatus.TIMED_OUT);
        return new GenericEventMessage(new MessageType(name), time,
                                       MetadataUtils.create(context.getWorkflowId(), stepName, StepStatus.TIMED_OUT)
        );
    }
}
