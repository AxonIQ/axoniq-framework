/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;

import static java.util.Objects.requireNonNull;

/**
 * The snapshot settings of the workflow engine, customized through {@link WorkflowConfigurer#snapshots}.
 * <p>
 * Snapshots are disabled by default. Once {@link #enabled(boolean) enabled}, the engine snapshots:
 * <ul>
 *     <li>The running-workflow list, as its {@link #runningWorkflows(SnapshotPolicy) snapshot policy} dictates. There
 *     is one list, so there is one snapshot of it.</li>
 *     <li>The state of each workflow instance, as its {@link #workflowState(SnapshotPolicy) snapshot policy} dictates.
 *     Every workflow instance has a snapshot of its own.</li>
 * </ul>
 * A restarting node then sources either from its latest snapshot instead of from every event ever published.
 * <p>
 * Example usage:
 * <pre>{@code
 * WorkflowConfigurer.create()
 *                   .snapshots(snapshots -> snapshots.enabled(true)
 *                                                    .runningWorkflows(SnapshotPolicy.afterEvents(500))
 *                                                    .workflowState(SnapshotPolicy.afterEvents(200)));
 * }</pre>
 *
 * @author Stefan Dragisic
 * @since 5.4.2
 */
public class WorkflowSnapshotConfiguration {

    private boolean enabled = false;
    private SnapshotPolicy runningWorkflows = SnapshotPolicy.afterEvents(100);
    private SnapshotPolicy workflowState = SnapshotPolicy.afterEvents(100);

    /**
     * Enables or disables snapshots. Defaults to {@code false}.
     *
     * @param enabled whether snapshots are taken
     * @return this configuration, for a fluent API
     */
    public WorkflowSnapshotConfiguration enabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Sets the {@link SnapshotPolicy} of the running-workflow list. Defaults to
     * {@link SnapshotPolicy#afterEvents(int) after 100 events}.
     *
     * @param runningWorkflows the {@link SnapshotPolicy} of the running-workflow list
     * @return this configuration, for a fluent API
     */
    public WorkflowSnapshotConfiguration runningWorkflows(SnapshotPolicy runningWorkflows) {
        this.runningWorkflows = requireNonNull(runningWorkflows, "The SnapshotPolicy must not be null.");
        return this;
    }

    /**
     * Sets the {@link SnapshotPolicy} of the state of each workflow instance. Defaults to
     * {@link SnapshotPolicy#afterEvents(int) after 100 events}.
     *
     * @param workflowState the {@link SnapshotPolicy} of the state of each workflow instance
     * @return this configuration, for a fluent API
     */
    public WorkflowSnapshotConfiguration workflowState(SnapshotPolicy workflowState) {
        this.workflowState = requireNonNull(workflowState, "The SnapshotPolicy must not be null.");
        return this;
    }

    /**
     * Returns whether snapshots are taken.
     *
     * @return {@code true} if snapshots are taken, {@code false} otherwise
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * Returns the {@link SnapshotPolicy} of the running-workflow list.
     *
     * @return the {@link SnapshotPolicy} of the running-workflow list
     */
    public SnapshotPolicy runningWorkflows() {
        return runningWorkflows;
    }

    /**
     * Returns the {@link SnapshotPolicy} of the state of each workflow instance.
     *
     * @return the {@link SnapshotPolicy} of the state of each workflow instance
     */
    public SnapshotPolicy workflowState() {
        return workflowState;
    }
}
