/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import org.axonframework.messaging.eventhandling.EventMessage;

/**
 * Workflow id provider using the message identifier as workflow id. This is the default provider. An event identifier
 * is a UUID unless the application assigns identifiers of its own, so the derived id is a valid workflow id, see
 * {@link WorkflowIdProvider}.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class MessageWorkflowIdProvider implements WorkflowIdProvider {

    @Override
    public String apply(EventMessage eventMessage) {
        return eventMessage.identifier();
    }
}
