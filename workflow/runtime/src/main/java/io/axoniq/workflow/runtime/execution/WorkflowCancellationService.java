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
package io.axoniq.workflow.runtime.execution;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Internal entry point for requesting cancellation of running workflows by identifier.
 * <p>
 * The service associates each live workflow id with its per-execution {@link WorkflowCancellation} coordinator. It
 * does not implement cancellation policy itself; it locates the coordinator that schedules cancellation safely on the
 * workflow control thread. A future user-facing workflow manager can delegate to this service without exposing the
 * execution implementation.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 0.3.0
 */
@Internal
public final class WorkflowCancellationService implements DescribableComponent {

    private final ConcurrentHashMap<String, WorkflowCancellation> cancellations = new ConcurrentHashMap<>();

    /**
     * Registers the cancellation coordinator for a live workflow.
     *
     * @param workflowId identifier of the live workflow
     * @param cancellation cancellation coordinator to register
     */
    void register(@Nonnull String workflowId, @Nonnull WorkflowCancellation cancellation) {
        cancellations.put(Objects.requireNonNull(workflowId, "Workflow id is mandatory"),
                          Objects.requireNonNull(cancellation, "Workflow cancellation is mandatory"));
    }

    /**
     * Removes the cancellation coordinator associated with a workflow.
     *
     * @param workflowId identifier of the workflow that is no longer live
     */
    void unregister(@Nonnull String workflowId) {
        cancellations.remove(Objects.requireNonNull(workflowId, "Workflow id is mandatory"));
    }

    /**
     * Removes every registered workflow cancellation coordinator.
     */
    void clear() {
        cancellations.clear();
    }

    /**
     * Requests cooperative cancellation of one workflow step.
     *
     * @param workflowId identifier of the workflow containing the step
     * @param stepName name of the step to cancel
     * @param cause optional reason for the cancellation
     * @return a future completing with {@code true} when a terminal step cancellation was recorded, or {@code false}
     * when the step was unknown or already terminal
     */
    @Nonnull
    public CompletableFuture<Boolean> cancelStep(@Nonnull String workflowId,
                                                 @Nonnull String stepName,
                                                 @Nullable Throwable cause) {
        return cancellationFor(workflowId).cancelStep(stepName, cause);
    }

    /**
     * Requests cooperative cancellation of every currently-running step without terminating the workflow.
     *
     * @param workflowId identifier of the workflow whose steps to cancel
     * @param cause optional reason for the cancellation
     * @return a future completing with the number of steps for which terminal cancellation was recorded
     */
    @Nonnull
    public CompletableFuture<Integer> cancelRunningSteps(@Nonnull String workflowId, @Nullable Throwable cause) {
        return cancellationFor(workflowId).cancelRunningSteps(cause);
    }

    /**
     * Requests cancellation of a workflow.
     *
     * @param workflowId identifier of the workflow to cancel
     * @param cause optional reason for the cancellation
     * @return a future completing after the workflow cancellation event is durable and the workflow body was woken
     */
    @Nonnull
    public CompletableFuture<Void> cancelWorkflow(@Nonnull String workflowId, @Nullable Throwable cause) {
        return cancellationFor(workflowId).cancelWorkflow(cause);
    }

    /**
     * Describes the workflow identifiers for which cancellation can currently be requested.
     *
     * @param descriptor descriptor receiving the component properties
     */
    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        descriptor.describeProperty("registeredWorkflowIds", cancellations.keySet().stream().toList());
    }

    @Nonnull
    private WorkflowCancellation cancellationFor(@Nonnull String workflowId) {
        var cancellation = cancellations.get(Objects.requireNonNull(workflowId, "Workflow id is mandatory"));
        if (cancellation == null) {
            throw new NoSuchElementException("No running workflow found with id '" + workflowId + "'");
        }
        return cancellation;
    }
}
