/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;

/**
 * {@link Message} implementation uniquely identifying a synthesized {@link WorkflowStatus} change, with its
 * {@link #payload() payload} being the {@link WorkflowStatus} the workflow transitioned into.
 *
 * @author Steven van Beelen
 * @since 5.4.0
 */
class WorkflowStatusChangeMessage extends GenericMessage {

    /**
     * Constructs a {@code WorkflowStatusChangeMessage} for the given {@code type} and {@code payload}.
     *
     * @param type    the {@link MessageType type} for this {@code WorkflowStatusChangeMessage}
     * @param payload the {@link WorkflowStatus} being resumed, as the payload for this
     *                {@code WorkflowStatusChangeMessage}
     */
    WorkflowStatusChangeMessage(MessageType type, WorkflowStatus payload) {
        super(type, payload);
    }
}
