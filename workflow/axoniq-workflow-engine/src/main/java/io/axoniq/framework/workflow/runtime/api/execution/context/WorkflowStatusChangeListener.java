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

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

/**
 * Functional interface describing a {@link WorkflowStatus} change listener, invoked on every status change.
 *
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 5.4.0
 */
@FunctionalInterface
public interface WorkflowStatusChangeListener {

    /**
     * Handler reacting on a {@link WorkflowStatus workflow status} change, including the entire {@code event} and its
     * {@code processingContext} which contain the status changed event.
     *
     * @param status            the workflow status change this handler reacts to
     * @param context           the current workflow context
     * @param event             event that triggered the status change
     * @param processingContext context in which the event is applied
     */
    void onWorkflowStatus(WorkflowStatus status,
                          WorkflowContext context,
                          EventMessage event,
                          ProcessingContext processingContext);
}
