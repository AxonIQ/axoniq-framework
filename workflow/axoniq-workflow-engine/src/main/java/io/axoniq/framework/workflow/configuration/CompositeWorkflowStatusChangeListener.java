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

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Composite Workflow status change listener responsible for one status change.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class CompositeWorkflowStatusChangeListener implements WorkflowStatusChangeListener {

    private final WorkflowStatus workflowStatus;
    private final List<WorkflowStatusChangeListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * Creates a new workflow status listener for certain workflow status.
     *
     * @param workflowStatus workflow status the listener will be propagating.
     */
    CompositeWorkflowStatusChangeListener(WorkflowStatus workflowStatus) {
        this.workflowStatus = workflowStatus;
    }

    /**
     * Adds a new listener.
     *
     * @param listener listener to add.
     */
    public void addListener(WorkflowStatusChangeListener listener) {
        this.listeners.add(listener);
    }

    /**
     * Removes a listener.
     *
     * @param listener listener to remove.
     */
    public void removeListener(WorkflowStatusChangeListener listener) {
        this.listeners.remove(listener);
    }

    /**
     * Checks if any listeners are present.
     *
     * @return false if at least one listener is present.
     */
    public boolean isEmpty() {
        return listeners.isEmpty();
    }

    @Override
    public void onWorkflowStatus(
            WorkflowStatus status,
            WorkflowContext workflowContext,
            EventMessage event,
            ProcessingContext processingContext
    ) {
        if (this.workflowStatus != status) {
            return;
        }

        this.listeners.forEach(l -> l.onWorkflowStatus(status, workflowContext, event, processingContext));
    }
}
