/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.jspecify.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Internal entry point for requesting cancellation of running workflows by identifier.
 * <p>
 * The service finds each workflow through the {@link LiveWorkflows} of its workflow module, which hands a request to
 * the per-execution {@link WorkflowCancellation} coordinator of the workflow's execution in memory, or wakes a
 * hibernated workflow for it. It does not implement cancellation policy itself; the coordinator schedules cancellation
 * safely on the workflow control thread. The workflow module creates one per module, from its {@link LiveWorkflows},
 * and the {@link SimpleWorkflowManager} delegates to it without exposing the execution implementation.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal
public final class WorkflowCancellationService implements DescribableComponent {

    private final LiveWorkflows liveWorkflows;

    /**
     * Creates the cancellation service of a workflow module, which finds the workflows to cancel among the given live
     * workflows. Internal, as the workflow module creates the service from the {@link LiveWorkflows} it shares with its
     * {@link WorkflowEngine}; a service created from other live workflows reaches none of the module's workflows.
     *
     * @param liveWorkflows the live workflows of the workflow module
     */
    @Internal
    public WorkflowCancellationService(LiveWorkflows liveWorkflows) {
        this.liveWorkflows = Objects.requireNonNull(liveWorkflows, "The LiveWorkflows must not be null.");
    }

    /**
     * Requests cooperative cancellation of one workflow step.
     * <p>
     * Blocks while a wake of the workflow loads it from the event store, up to the wake timeout of the module, as the
     * request finds the workflow under the lock that wake holds.
     *
     * @param workflowId identifier of the workflow containing the step
     * @param stepName   name of the step to cancel
     * @param cause      optional reason for the cancellation
     * @return a future completing with {@code true} when a terminal step cancellation was recorded, or {@code false}
     * when the step was unknown or already terminal
     * @throws NoSuchElementException when no workflow with the given identifier is running on this node, or it is
     *                                restored by a claim of its segment that did not catch up yet
     */
    public CompletableFuture<Boolean> requestStepCancellation(String workflowId,
                                                              String stepName,
                                                              @Nullable Throwable cause) {
        return liveWorkflows.request(requireWorkflowId(workflowId),
                                     cancellation -> cancellation.requestStepCancellation(stepName, cause));
    }

    /**
     * Requests cooperative cancellation of every currently-running step without terminating the workflow.
     * <p>
     * Blocks while a wake of the workflow loads it from the event store, up to the wake timeout of the module, as the
     * request finds the workflow under the lock that wake holds.
     *
     * @param workflowId identifier of the workflow whose steps to cancel
     * @param cause      optional reason for the cancellation
     * @return a future completing with the number of steps for which terminal cancellation was recorded
     * @throws NoSuchElementException when no workflow with the given identifier is running on this node, or it is
     *                                restored by a claim of its segment that did not catch up yet
     */
    public CompletableFuture<Integer> requestCancellationOfAllSteps(String workflowId,
                                                                    @Nullable Throwable cause) {
        return liveWorkflows.request(requireWorkflowId(workflowId),
                                     cancellation -> cancellation.requestCancellationOfAllSteps(cause));
    }

    /**
     * Requests cancellation of a workflow.
     * <p>
     * Blocks while a wake of the workflow loads it from the event store, up to the wake timeout of the module, as the
     * request finds the workflow under the lock that wake holds.
     *
     * @param workflowId identifier of the workflow to cancel
     * @param cause      optional reason for the cancellation
     * @return a future completing after the workflow cancellation event is durable and the workflow body was woken
     * @throws NoSuchElementException when no workflow with the given identifier is running on this node, or it is
     *                                restored by a claim of its segment that did not catch up yet
     */
    public CompletableFuture<Void> requestWorkflowCancellation(String workflowId, @Nullable Throwable cause) {
        return liveWorkflows.request(requireWorkflowId(workflowId),
                                     cancellation -> cancellation.requestWorkflowCancellation(cause));
    }

    /**
     * Describes the workflow identifiers for which cancellation can currently be requested.
     *
     * @param descriptor descriptor receiving the component properties
     */
    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("registeredWorkflowIds", liveWorkflows.cancellableWorkflowIds());
    }

    private static String requireWorkflowId(String workflowId) {
        return Objects.requireNonNull(workflowId, "Workflow id is mandatory");
    }
}
