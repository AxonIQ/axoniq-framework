/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.runtime.api.manager;

import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * Represents an operator of a workflow instance.
 *
 * @author Simon Zambrovski
 * @since 0.3.0
 */
public interface WorkflowInstanceOperator {

    /**
     * Requests cooperative cancellation of one workflow step.
     *
     * @param stepName name of the step to cancel
     * @param cause optional reason for the cancellation
     * @return future completing with {@code true} when a terminal step cancellation was recorded, or {@code false}
     * when the step was unknown or already terminal
     */
    CompletableFuture<Boolean> requestStepCancellation(String stepName, @Nullable Throwable cause);

    /**
     * Requests cooperative cancellation of every currently running step without terminating the workflow.
     *
     * @param cause optional reason for the cancellation
     * @return future completing with the number of steps for which terminal cancellation was recorded
     */
    CompletableFuture<Integer> requestCancellationOfAllSteps(@Nullable Throwable cause);

    /**
     * Requests cancellation of the workflow.
     *
     * @param cause optional reason for the cancellation
     * @return future completing after the workflow cancellation event is durable and the workflow body was woken
     */
    CompletableFuture<Void> requestWorkflowCancellation(@Nullable Throwable cause);
}
