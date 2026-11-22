/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * The snapshot settings of the workflow engine, customized through {@link WorkflowConfigurer#snapshots}, or registered
 * as a {@code WorkflowSnapshotConfiguration} component, such as the one Spring Boot creates from the
 * {@code axon.workflow.snapshots} properties. This component decides whether, and how, the engine snapshots its
 * entities.
 * <p>
 * The engine sources two event-sourced entities, and can snapshot both:
 * <ul>
 *     <li>The running-workflow list, as its {@link #runningWorkflows(SnapshotPolicy) snapshot policy} dictates. There
 *     is one list, so there is one snapshot of it.</li>
 *     <li>The state of each workflow instance, as its {@link #workflowState(SnapshotPolicy) snapshot policy} dictates.
 *     Every workflow instance has a snapshot of its own.</li>
 * </ul>
 * A restarting node, or a node claiming a segment, then sources either from its latest snapshot instead of from every
 * event ever published.
 * <p>
 * Whether snapshots are taken depends on {@link #enabled(boolean)}:
 * <ul>
 *     <li>Not set (the default): the state of each workflow instance is snapshotted when the application can take
 *     snapshots, that is, when it has a {@link org.axonframework.eventsourcing.snapshot.store.SnapshotStore}
 *     component, a {@link org.axonframework.conversion.GeneralConverter} component, and a
 *     {@link org.axonframework.messaging.core.MessageTypeResolver} that resolves the type of the workflow state.
 *     Otherwise no snapshot is taken, and the application starts as usual. The running-workflow list is not
 *     snapshotted. A {@link SnapshotPolicy} component registered under one of the deprecated names of
 *     {@link WorkflowConfigurationDefaults} still snapshots its entity with that policy, as it did before 5.5.0, and
 *     logs a warning.</li>
 *     <li>{@code true}: both entities are snapshotted. An application that cannot take snapshots fails to start.</li>
 *     <li>{@code false}: no snapshot is taken.</li>
 * </ul>
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

    @Nullable
    private Boolean enabled;
    private SnapshotPolicy runningWorkflows = SnapshotPolicy.afterEvents(50);
    private SnapshotPolicy workflowState = SnapshotPolicy.afterEvents(50);

    /**
     * Enables or disables snapshots. When not set, the state of each workflow instance is snapshotted whenever the
     * application can take snapshots, and the running-workflow list is not snapshotted.
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
     * {@link SnapshotPolicy#afterEvents(int) after 50 events}.
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
     * Returns whether snapshots are explicitly enabled through {@link #enabled(boolean)}.
     * <p>
     * Up to 5.4, snapshots were taken only when enabled, so this method told whether snapshots were taken. Since
     * 5.5.0, a {@code false} result no longer means that no snapshot is taken: unless snapshots are
     * {@link #disabled() disabled}, the state of each workflow instance is snapshotted whenever the application can
     * take snapshots.
     *
     * @return {@code true} if snapshots are explicitly enabled, {@code false} otherwise
     */
    public boolean enabled() {
        return Boolean.TRUE.equals(enabled);
    }

    /**
     * Returns whether snapshots are explicitly disabled through {@link #enabled(boolean)}.
     *
     * @return {@code true} if snapshots are explicitly disabled, {@code false} otherwise
     * @since 5.5.0
     */
    public boolean disabled() {
        return Boolean.FALSE.equals(enabled);
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
