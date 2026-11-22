/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link WorkflowStore} serving a fixed running-workflow list and recording which instances are sourced.
 * <p>
 * Sourcing an instance fails as an instance of another workflow module, so a restore quietly skips it, unless the
 * test gave the instance a {@link #withState(String, VersionedType, WorkflowStatus) state}: tests use the recorded ids
 * to see which instances a claim restores, without needing workflow definitions.
 *
 * @author Allard Buijze
 */
class RecordingWorkflowStore implements WorkflowStore {

    private final EventSourcedRunningWorkflows runningWorkflows = new EventSourcedRunningWorkflows();
    private final AtomicInteger runningWorkflowLoads = new AtomicInteger();
    private final List<String> sourcedWorkflowIds = new CopyOnWriteArrayList<>();
    private final Map<String, WorkflowState> states = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Void>> heldSourcing = new ConcurrentHashMap<>();
    @Nullable
    private volatile Throwable runningWorkflowLoadFailure;

    /**
     * Adds the given ids to the running-workflow list this store serves.
     *
     * @param workflowIds the ids of running workflows
     * @return this store, for chaining
     */
    RecordingWorkflowStore running(String... workflowIds) {
        for (String workflowId : workflowIds) {
            runningWorkflows.evolve(MetadataUtils.create(workflowId, WorkflowStatus.STARTED));
        }
        return this;
    }

    /**
     * Makes sourcing the given instance return a state of the given definition and status.
     *
     * @param workflowId   the id of the instance
     * @param definitionId the definition the state records
     * @param status       the status of the state
     * @return this store, for chaining
     */
    RecordingWorkflowStore withState(String workflowId, VersionedType definitionId, WorkflowStatus status) {
        var state = new EventSourcedWorkflowState(workflowId, definitionId);
        state.setStatus(status, null, false, new StubProcessingContext());
        states.put(workflowId, state);
        return this;
    }

    /**
     * Holds back sourcing the given instance until the returned future completes.
     *
     * @param workflowId the id of the instance
     * @return the future to complete to let sourcing the instance finish
     */
    CompletableFuture<Void> holdSourcing(String workflowId) {
        return heldSourcing.computeIfAbsent(workflowId, id -> new CompletableFuture<>());
    }

    /**
     * Makes every later load of the running-workflow list fail with the given {@code failure}.
     *
     * @param failure the failure the loads complete with
     * @return this store, for chaining
     */
    RecordingWorkflowStore failRunningWorkflowLoads(Throwable failure) {
        this.runningWorkflowLoadFailure = failure;
        return this;
    }

    /**
     * Returns how often the running-workflow list was loaded.
     *
     * @return the number of loads of the running-workflow list
     */
    int runningWorkflowLoads() {
        return runningWorkflowLoads.get();
    }

    /**
     * Returns the ids of the instances that were sourced, in order.
     *
     * @return the sourced instance ids
     */
    List<String> sourcedWorkflowIds() {
        return List.copyOf(sourcedWorkflowIds);
    }

    @Override
    public CompletableFuture<RunningWorkflows> loadRunningWorkflows(ProcessingContext processingContext) {
        runningWorkflowLoads.incrementAndGet();
        var failure = runningWorkflowLoadFailure;
        return failure != null
                ? CompletableFuture.failedFuture(failure)
                : CompletableFuture.completedFuture(runningWorkflows);
    }

    @Override
    public CompletableFuture<WorkflowState> loadWorkflow(String workflowId, ProcessingContext processingContext) {
        sourcedWorkflowIds.add(workflowId);
        var held = heldSourcing.get(workflowId);
        if (held != null) {
            return held.thenCompose(released -> sourced(workflowId));
        }
        return sourced(workflowId);
    }

    private CompletableFuture<WorkflowState> sourced(String workflowId) {
        var state = states.get(workflowId);
        if (state != null) {
            return CompletableFuture.completedFuture(state);
        }
        return CompletableFuture.failedFuture(
                new UnrecognizedWorkflowDefinitionException("No definition for '" + workflowId + "'")
        );
    }

    @Override
    public CompletableFuture<Optional<WorkflowState>> findWorkflow(String workflowId,
                                                                   ProcessingContext processingContext) {
        return CompletableFuture.completedFuture(Optional.empty());
    }
}
