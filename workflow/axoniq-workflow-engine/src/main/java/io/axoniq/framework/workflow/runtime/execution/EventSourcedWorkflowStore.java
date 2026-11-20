/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import org.axonframework.common.ClockUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.modelling.repository.ManagedEntity;
import org.axonframework.modelling.repository.Repository;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Event-sourcing repository-backed workflow store.
 * <p>
 * Loads the projection of workflow instances that have started without reaching a terminal state and the durable
 * event-sourced state for an individual workflow instance. With a {@link SnapshotStore}, it also keeps the
 * {@link HibernatedWorkflows} of each engine segment, as a snapshot without events behind it. Like an entity snapshot,
 * the record is the snapshot's payload: the {@link SnapshotStore} serializes it, and the given {@link Converter} turns
 * a loaded payload back into a record.
 * </p>
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class EventSourcedWorkflowStore implements WorkflowStore {

    /**
     * The name the {@link HibernatedWorkflows} are stored under in the {@link SnapshotStore}.
     */
    static final QualifiedName HIBERNATED_WORKFLOWS =
            new QualifiedName("io.axoniq.framework.workflow.HibernatedWorkflows");
    /**
     * The format version of a stored {@link HibernatedWorkflows}. A record of another version is ignored.
     */
    static final String HIBERNATED_WORKFLOWS_VERSION = "5.5.0";
    private static final Logger logger = LoggerFactory.getLogger(EventSourcedWorkflowStore.class);

    private final Repository<String, EventSourcedRunningWorkflows> runningWorkflowsRepository;
    private final Repository<String, EventSourcedWaitAssociations> waitAssociationsRepository;
    private final Repository<String, EventSourcedWorkflowState> workflowStateRepository;
    private final @Nullable SnapshotStore snapshotStore;
    private final Converter converter;

    /**
     * Creates a store that loads each workflow entity through its own repository.
     *
     * @param runningWorkflowsRepository the repository of the list of running workflows
     * @param waitAssociationsRepository the repository of the list of open waits
     * @param workflowStateRepository    the repository of the state of each workflow instance
     * @param snapshotStore              keeps the {@link HibernatedWorkflows}, or {@code null} to keep none, so a claim
     *                                   restores every running workflow
     * @param converter                  converts a loaded snapshot payload back into {@link HibernatedWorkflows}
     */
    public EventSourcedWorkflowStore(
            Repository<String, EventSourcedRunningWorkflows> runningWorkflowsRepository,
            Repository<String, EventSourcedWaitAssociations> waitAssociationsRepository,
            Repository<String, EventSourcedWorkflowState> workflowStateRepository,
            @Nullable SnapshotStore snapshotStore,
            Converter converter
    ) {
        this.runningWorkflowsRepository = Objects.requireNonNull(runningWorkflowsRepository,
                                                                 "Running workflows repository must not be null");
        this.waitAssociationsRepository = Objects.requireNonNull(waitAssociationsRepository,
                                                                 "Wait associations repository must not be null");
        this.workflowStateRepository = Objects.requireNonNull(workflowStateRepository,
                                                              "Workflow state repository must not be null");
        this.snapshotStore = snapshotStore;
        this.converter = Objects.requireNonNull(converter, "Converter must not be null");
    }

    @Override
    public CompletableFuture<RunningWorkflows> loadRunningWorkflows(ProcessingContext processingContext) {
        return runningWorkflowsRepository.loadOrCreate(EventSourcedRunningWorkflows.ENTITY_ID, processingContext)
                                         .thenApply(ManagedEntity::entity);
    }

    @Override
    public CompletableFuture<WaitAssociations> loadWaitAssociations(ProcessingContext processingContext) {
        return waitAssociationsRepository.loadOrCreate(EventSourcedWaitAssociations.ENTITY_ID, processingContext)
                                         .thenApply(ManagedEntity::entity);
    }

    @Override
    public CompletableFuture<WorkflowState> loadWorkflow(String workflowId,
                                                         ProcessingContext processingContext) {
        return workflowStateRepository.loadOrCreate(workflowId, processingContext)
                                      .thenApply(ManagedEntity::entity);
    }

    @Override
    public CompletableFuture<Optional<WorkflowState>> findWorkflow(String workflowId,
                                                                   ProcessingContext processingContext) {
        return workflowStateRepository.load(workflowId, processingContext)
                                      .thenApply(managed -> Optional.ofNullable(managed.entity()));
    }

    @Override
    public CompletableFuture<HibernatedWorkflows> loadHibernatedWorkflows(String identifier,
                                                                           ProcessingContext processingContext) {
        if (snapshotStore == null) {
            return CompletableFuture.completedFuture(HibernatedWorkflows.NONE);
        }
        return snapshotStore.load(HIBERNATED_WORKFLOWS, identifier, processingContext)
                            .thenApply(snapshot -> read(identifier, snapshot));
    }

    @Override
    public CompletableFuture<Void> storeHibernatedWorkflows(String identifier,
                                                            HibernatedWorkflows workflows,
                                                            ProcessingContext processingContext) {
        if (snapshotStore == null) {
            return CompletableFuture.completedFuture(null);
        }
        // No events lie behind this record. Snapshot stores accept a global index position, not the start position.
        var snapshot = new Snapshot(new GlobalIndexPosition(0),
                                    HIBERNATED_WORKFLOWS_VERSION,
                                    workflows,
                                    ClockUtils.instant(),
                                    Map.of());
        return snapshotStore.store(HIBERNATED_WORKFLOWS, identifier, snapshot, processingContext);
    }

    /**
     * Converts a loaded record back, the way an entity snapshot is converted back. One of another version counts as no
     * record, which only makes a claim restore those workflows. One that cannot be converted fails the load.
     */
    private HibernatedWorkflows read(String identifier, @Nullable Snapshot snapshot) {
        if (snapshot == null) {
            return HibernatedWorkflows.NONE;
        }
        if (!HIBERNATED_WORKFLOWS_VERSION.equals(snapshot.version())) {
            logger.info("Ignoring the hibernated workflows recorded under [{}]: version [{}] is not readable.",
                        identifier, snapshot.version());
            return HibernatedWorkflows.NONE;
        }
        var workflows = converter.convert(snapshot.payload(), HibernatedWorkflows.class);
        return workflows == null ? HibernatedWorkflows.NONE : workflows;
    }
}
