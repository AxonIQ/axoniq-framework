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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;

/**
 * Coordinates cancellation requests for one running workflow instance.
 * <p>
 * Cancellation is requested from arbitrary threads but executed by the workflow control thread. This preserves the
 * single-consumer ordering of workflow tasks and ensures that a completed future is followed by the appropriate
 * durable terminal event before the returned future completes.
 * This differs from {@link io.axoniq.workflow.runtime.api.execution.context.WorkflowLifecycleControl}, whose
 * operations apply lifecycle commands synchronously on the workflow control thread.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal
public interface WorkflowCancellation {

    /**
     * Requests cooperative cancellation of one running workflow step.
     *
     * @param stepName name of the step to cancel
     * @param cause optional reason for the cancellation
     * @return a future completing with {@code true} when a terminal step cancellation was recorded, or {@code false}
     * when the step was unknown or already terminal
     */
    CompletableFuture<Boolean> requestStepCancellation(String stepName, @Nullable Throwable cause);

    /**
     * Requests cooperative cancellation of every currently-running workflow step without terminating the workflow.
     *
     * @param cause optional reason for the cancellation
     * @return a future completing with the number of steps for which terminal cancellation was recorded
     */
    CompletableFuture<Integer> requestCancellationOfAllSteps(@Nullable Throwable cause);

    /**
     * Requests cancellation of the workflow and its local execution.
     *
     * @param cause optional reason for the cancellation
     * @return a future completing after the workflow cancellation event is durable and the workflow body was woken
     */
    CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause);

    /**
     * Provides pending workflow-cancellation requests to the workflow driver.
     * <p>
     * The cancellation coordinator owns this state. The workflow driver only consumes it after being woken so that it
     * can publish the terminal event on its single control thread.
     */
    interface Request extends WorkflowCancellation {

        /**
         * Returns whether a workflow cancellation request is waiting for the workflow driver.
         *
         * @return {@code true} when a cancellation request is pending, otherwise {@code false}
         */
        boolean hasPendingWorkflowCancellation();

        /**
         * Returns and marks the pending workflow cancellation request as consumed.
         *
         * @return the pending cancellation request, or {@code null} when no request is pending
         */
        @Nullable
        PendingRequest consumeWorkflowCancellation();

        /**
         * Aborts an unconsumed workflow cancellation request because its workflow execution is no longer live.
         *
         * @param reason reason the cancellation can no longer be performed
         */
        void abortPendingWorkflowCancellation(CancellationException reason);
    }

    /**
     * Represents one pending workflow-cancellation request owned by the cancellation coordinator.
     *
     * @param cause cancellation cause to record durably
     * @param callback future completed after the workflow driver performs the terminal transition
     */
    record PendingRequest(WorkflowCancelledException cause,
                          CompletableFuture<Void> callback) {
        public PendingRequest {
            Objects.requireNonNull(cause, "Cancellation cause must not be null");
            Objects.requireNonNull(callback, "Completion future must not be null");
        }

    }
}
