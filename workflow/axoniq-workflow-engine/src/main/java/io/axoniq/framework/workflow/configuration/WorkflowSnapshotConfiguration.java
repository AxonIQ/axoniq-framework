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
 * Snapshots are disabled by default. Once {@link #enabled(boolean) enabled}, the engine snapshots the state of each
 * workflow instance, as its {@link #workflowState(SnapshotPolicy) snapshot policy} dictates. A restarting node then
 * sources that state from its latest snapshot instead of from every event of the instance.
 * <p>
 * Example usage:
 * <pre>{@code
 * WorkflowConfigurer.create()
 *                   .snapshots(snapshots -> snapshots.enabled(true)
 *                                                    .workflowState(SnapshotPolicy.afterEvents(200)));
 * }</pre>
 *
 * @author Stefan Dragisic
 * @since 5.4.2
 */
public class WorkflowSnapshotConfiguration {

    private boolean enabled = false;
    private SnapshotPolicy runningWorkflows = SnapshotPolicy.afterEvents(50);
    private SnapshotPolicy workflowState = SnapshotPolicy.afterEvents(50);
    private SnapshotPolicy waitAssociations = SnapshotPolicy.afterEvents(50);

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
     * Sets the {@link SnapshotPolicy} of the running-workflow list, which has no effect.
     *
     * @param runningWorkflows the {@link SnapshotPolicy} of the running-workflow list
     * @return this configuration, for a fluent API
     * @deprecated the running-workflow list is no longer snapshotted; the workflow index replaces it
     */
    @Deprecated(forRemoval = true, since = "5.5.0")
    public WorkflowSnapshotConfiguration runningWorkflows(SnapshotPolicy runningWorkflows) {
        this.runningWorkflows = requireNonNull(runningWorkflows, "The SnapshotPolicy must not be null.");
        return this;
    }

    /**
     * Sets the {@link SnapshotPolicy} of the state of each workflow instance. Defaults to
     * {@link SnapshotPolicy#afterEvents(int) after 50 events}.
     *
     * @param workflowState the {@link SnapshotPolicy} of the state of each workflow instance
     * @return this configuration, for a fluent API
     */
    public WorkflowSnapshotConfiguration workflowState(SnapshotPolicy workflowState) {
        this.workflowState = requireNonNull(workflowState, "The SnapshotPolicy must not be null.");
        return this;
    }

    /**
     * Sets the {@link SnapshotPolicy} of the open-wait list, which has no effect.
     *
     * @param waitAssociations the {@link SnapshotPolicy} of the open-wait list
     * @return this configuration, for a fluent API
     * @since 5.5.0
     * @deprecated the open-wait list is no longer snapshotted; the workflow index replaces it
     */
    @Deprecated(forRemoval = true, since = "5.5.0")
    public WorkflowSnapshotConfiguration waitAssociations(SnapshotPolicy waitAssociations) {
        this.waitAssociations = requireNonNull(waitAssociations, "The SnapshotPolicy must not be null.");
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
     * Returns the {@link SnapshotPolicy} of the running-workflow list, which the engine does not use.
     *
     * @return the {@link SnapshotPolicy} of the running-workflow list
     * @deprecated the running-workflow list is no longer snapshotted; the workflow index replaces it
     */
    @Deprecated(forRemoval = true, since = "5.5.0")
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

    /**
     * Returns the {@link SnapshotPolicy} of the open-wait list, which the engine does not use.
     *
     * @return the {@link SnapshotPolicy} of the open-wait list
     * @since 5.5.0
     * @deprecated the open-wait list is no longer snapshotted; the workflow index replaces it
     */
    @Deprecated(forRemoval = true, since = "5.5.0")
    public SnapshotPolicy waitAssociations() {
        return waitAssociations;
    }
}
