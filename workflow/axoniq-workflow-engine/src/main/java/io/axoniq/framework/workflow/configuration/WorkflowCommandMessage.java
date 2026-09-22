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
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.core.MessageType;

/**
 * {@link CommandMessage} implementation uniquely identifying a synthesized request to resume/run a {@code @Workflow}
 * body, with it's {@link #payload() payload} being the {@link WorkflowContext} of the workflow instance.
 * <p>
 * Deliberately not a plain {@code CommandMessage}, as that would make
 * {@link io.axoniq.framework.workflow.runtime.api.annotation.Workflow} annotated handlers that are discovered with the
 * {@link AnnotatedWorkflowHandlerDefinition} seem like plain command handlers otherwise, which would register them with the
 * {@link org.axonframework.messaging.commandhandling.CommandBus}.
 *
 * @author Steven van Beelen
 * @since 5.4.0
 */
class WorkflowCommandMessage extends GenericCommandMessage {

    /**
     * Constructs a {@code WorkflowCommandMessage} for the given {@code type} and {@code payload}.
     *
     * @param type    the {@link MessageType type} for this {@code WorkflowCommandMessage}
     * @param payload the {@link WorkflowContext} being resumed, as the payload for this {@code WorkflowCommandMessage}
     */
    WorkflowCommandMessage(MessageType type, WorkflowContext payload) {
        super(type, payload);
    }
}
