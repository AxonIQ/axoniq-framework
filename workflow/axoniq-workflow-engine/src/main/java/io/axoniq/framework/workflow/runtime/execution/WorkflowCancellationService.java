/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.execution.LiveWorkflows.Hibernated;
import io.axoniq.framework.workflow.runtime.execution.LiveWorkflows.InMemory;
import io.axoniq.framework.workflow.runtime.execution.LiveWorkflows.LiveWorkflow;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Internal entry point for requesting cancellation of running workflows by identifier.
 * <p>
 * The service routes each request to the workflow wherever it is on this node:
 * <ul>
 *     <li>under the lock of the workflow, it reads the entry of the workflow in the {@link LiveWorkflows} of its
 *     workflow module, and follows the {@link CancellationRoute} of an execution in memory: made against the
 *     per-execution {@link WorkflowCancellation} coordinator, or queued as a task of an execution that woke from
 *     hibernation;</li>
 *     <li>after that lock, for a hibernated workflow, it has the {@link Hibernation} of the module's
 *     {@link WorkflowEngine} wake the workflow, and queues the request on the woken execution.</li>
 * </ul>
 * Under the lock of the workflow, a request either reaches the execution before it checks for a safe point, or finds
 * the workflow hibernated, never neither. It does not implement cancellation policy itself; the coordinator schedules
 * cancellation safely on the workflow control thread. The workflow module creates one per module, from its
 * {@link LiveWorkflows} and its {@link WorkflowEngine}, and the {@link SimpleWorkflowManager} delegates to it without
 * exposing the execution implementation.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal
public final class WorkflowCancellationService implements DescribableComponent {

    private final LiveWorkflows liveWorkflows;
    private final Supplier<Hibernation> hibernation;

    /**
     * Creates the cancellation service of a workflow module, which finds the workflows to cancel among the given live
     * workflows, and wakes a hibernated one through the supplied {@link Hibernation}. Internal, as the workflow module
     * creates the service from the {@link LiveWorkflows} it shares with its {@link WorkflowEngine}, and supplies the
     * {@link WorkflowEngine#hibernation() hibernation} of that engine; a service created from other live workflows
     * reaches none of the module's workflows.
     * <p>
     * The hibernation is supplied, and read for each request a hibernated workflow gets, as the engine enables
     * hibernation only as it starts, after the module created this service.
     *
     * @param liveWorkflows the live workflows of the workflow module
     * @param hibernation   supplies the hibernation of the engine of the workflow module, which wakes a hibernated
     *                      workflow for a request
     */
    @Internal
    public WorkflowCancellationService(LiveWorkflows liveWorkflows, Supplier<Hibernation> hibernation) {
        this.liveWorkflows = Objects.requireNonNull(liveWorkflows, "The LiveWorkflows must not be null.");
        this.hibernation = Objects.requireNonNull(hibernation, "The Hibernation supplier must not be null.");
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
        return request(requireWorkflowId(workflowId),
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
        return request(requireWorkflowId(workflowId),
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
        return request(requireWorkflowId(workflowId), cancellation -> cancellation.requestWorkflowCancellation(cause));
    }

    /**
     * Describes the workflow identifiers for which cancellation can currently be requested.
     *
     * @param descriptor descriptor receiving the component properties
     */
    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("registeredWorkflowIds", cancellableWorkflowIds());
    }

    /**
     * Returns the identifiers of the live workflows a cancellation can be requested for: the hibernated ones, and those
     * in memory whose {@link CancellationRoute route} takes requests.
     */
    private List<String> cancellableWorkflowIds() {
        return liveWorkflows.entries()
                            .filter(entry -> !(entry instanceof InMemory inMemory)
                                    || inMemory.cancellationRoute().takesRequests())
                            .map(LiveWorkflow::workflowId)
                            .toList();
    }

    /**
     * Makes the given request of the given workflow, wherever the workflow is on this node.
     * <p>
     * Under the lock of the workflow, the request follows the {@link CancellationRoute} of an execution in memory,
     * see {@link CancellationRoute#request(CancellationRequest)}. As a wake of the workflow holds that lock while it
     * loads the workflow from the event store, a request waits for that load, up to the wake timeout.
     * <p>
     * For a hibernated workflow, the request is made once the lock is released, see
     * {@link #requestWhileHibernated(String, QueuedRequest)}: a wake for a deadline waits for a permit to load before
     * it takes the lock of the workflow, so a wake executor that runs the wake on the requesting thread must find that
     * lock free.
     * <p>
     * A request queued on an execution that leaves memory before its body took it is made again through this method,
     * so it finds the workflow wherever it is then, hibernated included.
     *
     * @throws NoSuchElementException when the workflow is not live here, or its execution takes no cancellation
     *                                requests yet, as it is restored by a claim that did not catch up
     */
    private <T> CompletableFuture<T> request(String workflowId,
                                             Function<WorkflowCancellation, CompletableFuture<T>> against) {
        var request = new CancellationRequest<>(against, () -> request(workflowId, against));
        try (var ignored = liveWorkflows.lock(workflowId)) {
            switch (liveWorkflows.get(workflowId)) {
                case null -> throw noRunningWorkflow(workflowId);
                case InMemory entry -> {
                    return entry.cancellationRoute()
                                .request(request)
                                .orElseThrow(() -> noRunningWorkflow(workflowId));
                }
                case Hibernated hibernated -> {
                    // Woken once the lock is released, below.
                }
            }
        }
        return requestWhileHibernated(workflowId, new QueuedRequest<>(request));
    }

    /**
     * Makes the given request of the given hibernated workflow: has the {@link Hibernation} of the engine wake the
     * workflow for it, on its wake executor once the hibernation may wake, and queues the request as a task of the
     * woken execution before its body starts, the way an event that wakes a workflow is handed to it. A woken body
     * takes no task before it waits again where it hibernated, as every earlier wait is answered from its recorded
     * state, so the body makes the request once it replayed its steps and they are registered again.
     * <p>
     * When the wake did not queue the request, the request is answered once the wake ran, see
     * {@link QueuedRequest#answerUnqueued(String, Throwable, boolean)}.
     */
    private <T> CompletableFuture<T> requestWhileHibernated(String workflowId, QueuedRequest<T> queued) {
        var queuedOnWoken = new AtomicBoolean();
        // Not queued when the woken execution left memory again before this ran, which takes no lock, so it has no
        // entry. The entry of a woken execution has a queued route.
        hibernation.get().wakeForCancellation(workflowId, woken -> liveWorkflows.entryOf(woken).ifPresent(entry -> {
            if (entry.cancellationRoute().queue(queued)) {
                queuedOnWoken.set(true);
            }
        })).whenComplete((ignored, wakeFailure) -> {
            if (!queuedOnWoken.get()) {
                queued.answerUnqueued(workflowId, wakeFailure, liveWorkflows.isHibernated(workflowId));
            }
        });
        return queued.result();
    }

    /**
     * Returns the exception a request for a workflow that takes no requests on this node fails with.
     */
    private static NoSuchElementException noRunningWorkflow(String workflowId) {
        return new NoSuchElementException("No running workflow found with id '" + workflowId + "'");
    }

    private static String requireWorkflowId(String workflowId) {
        return Objects.requireNonNull(workflowId, "Workflow id is mandatory");
    }
}
