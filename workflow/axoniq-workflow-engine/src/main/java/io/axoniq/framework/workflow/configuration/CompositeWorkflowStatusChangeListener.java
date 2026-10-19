/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

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
            ProcessingContext processingContext
    ) {
        if (this.workflowStatus != status) {
            return;
        }

        this.listeners.forEach(l -> l.onWorkflowStatus(status, workflowContext, processingContext));
    }
}
