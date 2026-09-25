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
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import org.axonframework.messaging.eventhandling.EventMessage;

/**
 * Primitive publishing a business event as a durable workflow step.
 * <p>
 * Exactly one event is appended: the given {@link EventMessage} itself, enriched with the workflow metadata
 * ({@code workflowId}, {@code stepName}, {@code stepType=COMPLETED}, {@code stepPrimitive=PUBLISH}). The event's
 * {@link org.axonframework.messaging.core.MessageType}, payload, identifier and timestamp are left untouched, so the
 * event consumers receive is the event the workflow published. Workflow metadata keys override user metadata keys of
 * the same name.
 * <p>
 * The appended event acts both as the business event and as the replay-safe checkpoint of the step: on replay the step
 * is found in the workflow state and the event is not published again. Because the event is routed like any other
 * business event, other workflows may start on it or be woken by it.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public interface PublishPrimitive {

    /**
     * Publishes the event carried by the given {@code command} as a durable step and waits until the step is recorded
     * in the workflow state. On replay, or when the step is already recorded, nothing is published.
     *
     * @param command parameter object carrying the {@code stepName} and the {@link EventMessage} to publish
     * @return a {@link WorkflowStepResult} that resolves once the published step is part of the workflow state
     */
    WorkflowStepResult publish(PublishCommand command);

    /**
     * Parameter object for the primitive.
     */
    interface PublishCommand {

        /**
         * Logical step name recorded with the published event. Forms the durable identifier of the step.
         *
         * @return step name
         */
        String stepName();

        /**
         * Event to publish. Its type, payload, identifier and timestamp are published as-is.
         *
         * @return event message
         */
        EventMessage event();
    }
}
