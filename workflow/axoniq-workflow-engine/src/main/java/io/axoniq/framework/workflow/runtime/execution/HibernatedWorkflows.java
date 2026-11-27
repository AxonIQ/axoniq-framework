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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The workflows of one segment that hibernated, as the rows of a {@link WorkflowIndex} hold them.
 * <p>
 * A workflow counts as hibernated only while its open waits are exactly the ones it hibernated with, so a row that is
 * outdated never hides a workflow that must run.
 * <p>
 * Internal, because it is part of how the {@link WorkflowEngine} restores the workflows of a claimed segment.
 *
 * @param workflows the hibernated workflows by workflow identifier
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public record HibernatedWorkflows(Map<String, HibernatedWorkflow> workflows) {

    /**
     * No hibernated workflows, for a segment without wake times.
     */
    public static final HibernatedWorkflows NONE = new HibernatedWorkflows(Map.of());

    /**
     * Creates a record, copying the given workflows.
     *
     * @param workflows the hibernated workflows by workflow identifier
     */
    public HibernatedWorkflows {
        workflows = Map.copyOf(Objects.requireNonNull(workflows, "The workflows must not be null."));
    }

    /**
     * Returns the given workflow when it is still hibernated: its open waits are exactly the ones it hibernated with.
     *
     * @param workflowId the identifier of the workflow
     * @param openWaits  the step names of the workflow's open waits in the event store
     * @return the hibernated workflow, or empty when it is not hibernated or its open waits changed since
     */
    public Optional<HibernatedWorkflow> hibernated(String workflowId, Set<String> openWaits) {
        var workflow = workflows.get(workflowId);
        return workflow != null && workflow.openWaits().equals(openWaits) ? Optional.of(workflow) : Optional.empty();
    }

    /**
     * A workflow that hibernated.
     *
     * @param openWaits the step names of the open waits it hibernated with
     * @param wakesAt   the nearest timeout of those waits, which wakes it
     */
    public record HibernatedWorkflow(Set<String> openWaits, Instant wakesAt) {

        /**
         * Creates a hibernated workflow, copying the given wait names.
         *
         * @param openWaits the step names of the open waits it hibernated with
         * @param wakesAt   the nearest timeout of those waits, which wakes it
         */
        public HibernatedWorkflow {
            openWaits = Set.copyOf(Objects.requireNonNull(openWaits, "The open waits must not be null."));
            Objects.requireNonNull(wakesAt, "The wake time must not be null.");
        }
    }
}
