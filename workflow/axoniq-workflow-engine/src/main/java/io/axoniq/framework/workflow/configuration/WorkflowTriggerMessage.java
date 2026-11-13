/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;

/**
 * {@link Message} implementation uniquely identifying a synthesized request to resume/run a {@code @Workflow} body,
 * with its {@link #payload() payload} being the {@link WorkflowContext} of the workflow instance.
 *
 * @author Steven van Beelen
 * @since 5.4.0
 */
class WorkflowTriggerMessage extends GenericMessage {

    /**
     * Constructs a {@code WorkflowTriggerMessage} for the given {@code type} and {@code payload}.
     *
     * @param type    the {@link MessageType type} for this {@code WorkflowTriggerMessage}
     * @param payload the {@link WorkflowContext} being resumed, as the payload for this {@code WorkflowTriggerMessage}
     */
    WorkflowTriggerMessage(MessageType type, WorkflowContext payload) {
        super(type, payload);
    }
}
