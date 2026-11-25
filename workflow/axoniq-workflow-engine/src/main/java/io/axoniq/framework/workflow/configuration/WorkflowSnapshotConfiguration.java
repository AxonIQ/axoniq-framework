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
 * The engine sources three event-sourced entities, and can snapshot each:
 * <ul>
 *     <li>The running-workflow list, as its {@link #runningWorkflows(SnapshotPolicy) snapshot policy} dictates. There
 *     is one list, so there is one snapshot of it.</li>
 *     <li>The state of each workflow instance, as its {@link #workflowState(SnapshotPolicy) snapshot policy} dictates.
 *     Every workflow instance has a snapshot of its own.</li>
 *     <li>The open-wait list, as its {@link #waitAssociations(SnapshotPolicy) snapshot policy} dictates. There is one
 *     list, so there is one snapshot of it. Every segment claim loads this list.</li>
 * </ul>
 * A restarting node, or a node claiming a segment, then sources each from its latest snapshot instead of from every
 * event ever published.
 * <p>
 * Whether snapshots are taken depends on {@link #enabled(boolean)}:
 * <ul>
 *     <li>Not set (the default): an entity is snapshotted when the application can take snapshots, that is, when it
 *     has a {@link org.axonframework.eventsourcing.snapshot.store.SnapshotStore} component, a
 *     {@link org.axonframework.conversion.GeneralConverter} component, and a
 *     {@link org.axonframework.messaging.core.MessageTypeResolver} that resolves the type of the entity. Otherwise no
 *     snapshot is taken, and the application starts as usual.</li>
 *     <li>{@code true}: every entity is snapshotted. An application that cannot take snapshots fails to start.</li>
 *     <li>{@code false}: no snapshot is taken.</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>{@code
 * WorkflowConfigurer.create()
 *                   .snapshots(snapshots -> snapshots.enabled(true)
 *                                                    .runningWorkflows(SnapshotPolicy.afterEvents(500))
 *                                                    .workflowState(SnapshotPolicy.afterEvents(200))
 *                                                    .waitAssociations(SnapshotPolicy.afterEvents(500)));
 * }</pre>
 *
 * @author Stefan Dragisic
 * @since 5.4.2
 */
public class WorkflowSnapshotConfiguration {

    private @Nullable Boolean enabled;
    private SnapshotPolicy runningWorkflows = SnapshotPolicy.afterEvents(50);
    private SnapshotPolicy workflowState = SnapshotPolicy.afterEvents(50);
    private SnapshotPolicy waitAssociations = SnapshotPolicy.afterEvents(50);

    /**
     * Enables or disables snapshots.
     * <p>
     * When this is not set, snapshots are taken if the application has a
     * {@link org.axonframework.eventsourcing.snapshot.store.SnapshotStore}, and are not taken otherwise. Set to
     * {@code true}, snapshots are taken and the application needs a {@code SnapshotStore} to start. Set to
     * {@code false}, no snapshots are taken.
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
     * Sets the {@link SnapshotPolicy} of the open-wait list. Defaults to
     * {@link SnapshotPolicy#afterEvents(int) after 50 events}.
     *
     * @param waitAssociations the {@link SnapshotPolicy} of the open-wait list
     * @return this configuration, for a fluent API
     * @since 5.5.0
     */
    public WorkflowSnapshotConfiguration waitAssociations(SnapshotPolicy waitAssociations) {
        this.waitAssociations = requireNonNull(waitAssociations, "The SnapshotPolicy must not be null.");
        return this;
    }

    /**
     * Returns whether snapshots are explicitly enabled.
     *
     * @return {@code true} if snapshots were enabled through {@link #enabled(boolean)}, {@code false} when they were
     * disabled or not set
     */
    public boolean enabled() {
        return Boolean.TRUE.equals(enabled);
    }

    /**
     * Returns whether snapshots are explicitly disabled.
     *
     * @return {@code true} if snapshots were disabled through {@link #enabled(boolean)}, {@code false} when they were
     * enabled or not set
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

    /**
     * Returns the {@link SnapshotPolicy} of the open-wait list.
     *
     * @return the {@link SnapshotPolicy} of the open-wait list
     * @since 5.5.0
     */
    public SnapshotPolicy waitAssociations() {
        return waitAssociations;
    }
}
