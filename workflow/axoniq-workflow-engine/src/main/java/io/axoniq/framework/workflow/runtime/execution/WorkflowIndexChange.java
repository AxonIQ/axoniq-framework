/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;

import java.time.Instant;
import java.util.Objects;

/**
 * One change to the rows of a {@link WorkflowIndex}. Storing a change twice has the same outcome as storing it once.
 * Internal, because the {@link WorkflowEngine} uses it to keep the index current.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public sealed interface WorkflowIndexChange {

    /**
     * Returns the identifier of the workflow the change is about.
     *
     * @return the identifier of the workflow
     */
    String workflowId();

    /**
     * A workflow started: its row is added, unless it is there.
     *
     * @param workflowId the identifier of the workflow
     */
    record WorkflowStarted(String workflowId) implements WorkflowIndexChange {

        /**
         * Creates the change for a started workflow.
         *
         * @param workflowId the identifier of the workflow
         */
        public WorkflowStarted {
            Objects.requireNonNull(workflowId, "The workflow id must not be null.");
        }
    }

    /**
     * A workflow ended: its row and the rows of its waits are removed.
     *
     * @param workflowId the identifier of the workflow
     */
    record WorkflowEnded(String workflowId) implements WorkflowIndexChange {

        /**
         * Creates the change for an ended workflow.
         *
         * @param workflowId the identifier of the workflow
         */
        public WorkflowEnded {
            Objects.requireNonNull(workflowId, "The workflow id must not be null.");
        }
    }

    /**
     * A wait opened: its row replaces the row of the same workflow and step, if there is one.
     *
     * @param association the open wait
     */
    record WaitOpened(WaitAssociation association) implements WorkflowIndexChange {

        /**
         * Creates the change for an opened wait.
         *
         * @param association the open wait
         */
        public WaitOpened {
            Objects.requireNonNull(association, "The wait association must not be null.");
        }

        @Override
        public String workflowId() {
            return association.workflowId();
        }
    }

    /**
     * A wait closed: its row is removed.
     *
     * @param workflowId the identifier of the workflow
     * @param stepName   the name of the wait step
     */
    record WaitClosed(String workflowId, String stepName) implements WorkflowIndexChange {

        /**
         * Creates the change for a closed wait.
         *
         * @param workflowId the identifier of the workflow
         * @param stepName   the name of the wait step
         */
        public WaitClosed {
            Objects.requireNonNull(workflowId, "The workflow id must not be null.");
            Objects.requireNonNull(stepName, "The step name must not be null.");
        }
    }

    /**
     * A workflow hibernated: its row gets the time it wakes at, and is added if its start is not stored yet.
     *
     * @param workflowId the identifier of the workflow
     * @param wakesAt    the nearest timeout of its open waits, which wakes it
     */
    record WakesAtSet(String workflowId, Instant wakesAt) implements WorkflowIndexChange {

        /**
         * Creates the change for a hibernated workflow.
         *
         * @param workflowId the identifier of the workflow
         * @param wakesAt    the nearest timeout of its open waits, which wakes it
         */
        public WakesAtSet {
            Objects.requireNonNull(workflowId, "The workflow id must not be null.");
            Objects.requireNonNull(wakesAt, "The wake time must not be null.");
        }
    }

    /**
     * A workflow woke: its row loses the time it wakes at. A missing row stays missing, as the workflow may have ended.
     *
     * @param workflowId the identifier of the workflow
     */
    record WakesAtCleared(String workflowId) implements WorkflowIndexChange {

        /**
         * Creates the change for a workflow that woke.
         *
         * @param workflowId the identifier of the workflow
         */
        public WakesAtCleared {
            Objects.requireNonNull(workflowId, "The workflow id must not be null.");
        }
    }
}
