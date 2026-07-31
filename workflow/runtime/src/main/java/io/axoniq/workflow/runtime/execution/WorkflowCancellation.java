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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;

import java.util.concurrent.CompletableFuture;

/**
 * Coordinates cancellation requests for one running workflow instance.
 * <p>
 * Cancellation is requested from arbitrary threads but executed by the workflow control thread. This preserves the
 * single-consumer ordering of workflow tasks and ensures that a completed future is followed by the appropriate
 * durable terminal event before the returned future completes.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 0.3.0
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
    @Nonnull
    CompletableFuture<Boolean> cancelStep(@Nonnull String stepName, @Nullable Throwable cause);

    /**
     * Requests cooperative cancellation of every currently-running workflow step without terminating the workflow.
     *
     * @param cause optional reason for the cancellation
     * @return a future completing with the number of steps for which terminal cancellation was recorded
     */
    @Nonnull
    CompletableFuture<Integer> cancelRunningSteps(@Nullable Throwable cause);

    /**
     * Requests cancellation of the workflow and its local execution.
     *
     * @param cause optional reason for the cancellation
     * @return a future completing after the workflow cancellation event is durable and the workflow body was woken
     */
    @Nonnull
    CompletableFuture<Void> cancelWorkflow(@Nullable Throwable cause);

    /**
     * Provides the pending external workflow-cancellation request to the workflow driver.
     * <p>
     * The cancellation coordinator owns this state. The workflow driver only consumes it after being woken so that it
     * can publish the terminal event on its single control thread.
     */
    interface External extends WorkflowCancellation {

        /**
         * Returns whether an external workflow cancellation is waiting for the workflow driver.
         *
         * @return {@code true} when a cancellation request is pending, otherwise {@code false}
         */
        boolean hasPendingWorkflowCancellation();

        /**
         * Removes and returns the pending external workflow cancellation.
         *
         * @return the pending cancellation request, or {@code null} when no request is pending
         */
        @Nullable
        Request consumeWorkflowCancellation();
    }

    /**
     * Represents one external workflow-cancellation request owned by the cancellation coordinator.
     *
     * @param cause cancellation cause to record durably
     * @param done future completed after the workflow driver performs the terminal transition
     */
    record Request(@Nonnull WorkflowCancelledException cause,
                   @Nonnull CompletableFuture<Void> done) {

    }
}
