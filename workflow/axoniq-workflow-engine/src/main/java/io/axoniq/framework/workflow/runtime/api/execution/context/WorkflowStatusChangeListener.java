/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

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
     * Handler reacting on a {@link WorkflowStatus workflow status} change.
     *
     * @param status            the workflow status change this handler reacts to
     * @param context           the current workflow context
     * @param processingContext context in which the status change is applied
     */
    void onWorkflowStatus(WorkflowStatus status,
                          WorkflowContext context,
                          ProcessingContext processingContext);
}
