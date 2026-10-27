/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

/**
 * The change a workflow event makes to the open waits of its workflow.
 * <p>
 * Shared by the durable {@link EventSourcedWaitAssociations} and the live {@link WaitAssociationIndex}, so both read a
 * workflow's events the same way:
 * <ul>
 *     <li>a wait-step STARTED event opens a wait,</li>
 *     <li>any step event with a terminal step status closes the wait of that step, whether or not it carries the wait
 *     marker, which also covers cancelled waits recorded without it,</li>
 *     <li>a lifecycle event with a terminal workflow status closes every wait of the workflow, because a terminal
 *     workflow writes no step events for the waits it interrupts.</li>
 * </ul>
 * Internal, because it only organizes the reading of workflow events for the wait association index.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
sealed interface WaitAssociationChange {

    /**
     * Returns the change the given event makes to the open waits of its workflow.
     *
     * @param event the event to inspect
     * @return the change, or {@code null} when the event opens or closes no wait
     */
    static @Nullable WaitAssociationChange of(EventMessage event) {
        var metadata = event.metadata();
        if (!MetadataUtils.hasWorkflowId().test(metadata)) {
            return null;
        }
        var workflowId = MetadataUtils.getWorkflowId(metadata);
        if (MetadataUtils.getWorkflowStatus(metadata).filter(WorkflowStatus::isTerminal).isPresent()) {
            return new WorkflowTerminated(workflowId);
        }
        var stepName = MetadataUtils.getStepName(metadata);
        var stepStatus = MetadataUtils.getStepStatus(metadata).orElse(null);
        if (stepName == null || stepStatus == null) {
            return null;
        }
        if (stepStatus == StepStatus.STARTED) {
            return MetadataUtils.isWaitForEventStep(metadata)
                    ? new WaitStarted(WaitAssociation.fromStartedEvent(event))
                    : null;
        }
        return stepStatus.isTerminal() ? new WaitTerminated(workflowId, stepName) : null;
    }

    /**
     * A wait was opened.
     *
     * @param row the row of the opened wait
     */
    record WaitStarted(WaitAssociation row) implements WaitAssociationChange {

    }

    /**
     * A step reached a terminal status, which closes its wait if the step was a wait.
     *
     * @param workflowId the identifier of the workflow
     * @param stepName   the name of the step that reached a terminal status
     */
    record WaitTerminated(String workflowId, String stepName) implements WaitAssociationChange {

    }

    /**
     * The workflow reached a terminal status, which closes all its waits.
     *
     * @param workflowId the identifier of the workflow
     */
    record WorkflowTerminated(String workflowId) implements WaitAssociationChange {

    }
}
