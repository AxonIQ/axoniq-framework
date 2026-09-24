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
