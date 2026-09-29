/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.jspecify.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Internal entry point for requesting cancellation of running workflows by identifier.
 * <p>
 * The service associates each live workflow id with its per-execution {@link WorkflowCancellation} coordinator. It does
 * not implement cancellation policy itself; it locates the coordinator that schedules cancellation safely on the
 * workflow control thread. A future user-facing workflow manager can delegate to this service without exposing the
 * execution implementation.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal
public final class WorkflowCancellationService implements DescribableComponent {

    private final ConcurrentHashMap<String, WorkflowCancellation> cancellations = new ConcurrentHashMap<>();

    private static void abortPendingWorkflowCancellation(@Nullable WorkflowCancellation cancellation,
                                                         String reason) {
        if (cancellation instanceof WorkflowCancellation.Request request) {
            request.abortPendingWorkflowCancellation(new CancellationException(reason));
        }
    }

    /**
     * Registers the cancellation coordinator for a live workflow.
     *
     * @param workflowId   identifier of the live workflow
     * @param cancellation cancellation coordinator to register
     */
    void register(String workflowId, WorkflowCancellation cancellation) {
        cancellations.put(Objects.requireNonNull(workflowId, "Workflow id is mandatory"),
                          Objects.requireNonNull(cancellation, "Workflow cancellation is mandatory"));
    }

    /**
     * Removes the cancellation coordinator associated with a workflow.
     *
     * @param workflowId identifier of the workflow that is no longer live
     */
    void unregister(String workflowId) {
        var cancellation = cancellations.remove(Objects.requireNonNull(workflowId, "Workflow id is mandatory"));
        abortPendingWorkflowCancellation(cancellation,
                                         "Workflow execution completed before cancellation was performed");
    }

    /**
     * Removes every registered workflow cancellation coordinator.
     */
    void clear() {
        cancellations.values().forEach(cancellation -> abortPendingWorkflowCancellation(
                cancellation, "Workflow engine shut down before cancellation was performed"));
        cancellations.clear();
    }

    /**
     * Requests cooperative cancellation of one workflow step.
     *
     * @param workflowId identifier of the workflow containing the step
     * @param stepName   name of the step to cancel
     * @param cause      optional reason for the cancellation
     * @return a future completing with {@code true} when a terminal step cancellation was recorded, or {@code false}
     * when the step was unknown or already terminal
     */
    public CompletableFuture<Boolean> requestStepCancellation(String workflowId,
                                                              String stepName,
                                                              @Nullable Throwable cause) {
        return cancellationFor(workflowId).requestStepCancellation(stepName, cause);
    }

    /**
     * Requests cooperative cancellation of every currently-running step without terminating the workflow.
     *
     * @param workflowId identifier of the workflow whose steps to cancel
     * @param cause      optional reason for the cancellation
     * @return a future completing with the number of steps for which terminal cancellation was recorded
     */
    public CompletableFuture<Integer> requestCancellationOfAllSteps(String workflowId,
                                                                    @Nullable Throwable cause) {
        return cancellationFor(workflowId).requestCancellationOfAllSteps(cause);
    }

    /**
     * Requests cancellation of a workflow.
     *
     * @param workflowId identifier of the workflow to cancel
     * @param cause      optional reason for the cancellation
     * @return a future completing after the workflow cancellation event is durable and the workflow body was woken
     */
    public CompletableFuture<Void> requestWorkflowCancellation(String workflowId, @Nullable Throwable cause) {
        return cancellationFor(workflowId).requestWorkflowCancellation(cause);
    }

    /**
     * Describes the workflow identifiers for which cancellation can currently be requested.
     *
     * @param descriptor descriptor receiving the component properties
     */
    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("registeredWorkflowIds", cancellations.keySet().stream().toList());
    }

    private WorkflowCancellation cancellationFor(String workflowId) {
        var cancellation = cancellations.get(Objects.requireNonNull(workflowId, "Workflow id is mandatory"));
        if (cancellation == null) {
            throw new NoSuchElementException("No running workflow found with id '" + workflowId + "'");
        }
        return cancellation;
    }
}
