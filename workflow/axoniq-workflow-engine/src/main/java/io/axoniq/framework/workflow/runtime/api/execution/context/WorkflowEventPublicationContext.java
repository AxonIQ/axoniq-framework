/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.execution.context;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * Read-only execution data required to create workflow event messages.
 *
 * <p>This internal contract deliberately excludes execution primitives. It lets event-message construction depend on
 * the values it needs without depending on the runtime's primitive-operation surface.</p>
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public interface WorkflowEventPublicationContext {

    /**
     * Returns the unique workflow execution identifier.
     *
     * @return the workflow identifier
     */
    String workflowId();

    /**
     * Returns the workflow definition version currently used by this execution.
     *
     * @return the workflow definition version
     */
    String workflowVersion();

    /**
     * Returns the current workflow payload.
     *
     * @return the workflow payload
     */
    Map<String, @Nullable Object> workflowPayload();

    /**
     * Returns the processing context of the current workflow execution.
     *
     * @return the processing context
     */
    ProcessingContext processingContext();
}
