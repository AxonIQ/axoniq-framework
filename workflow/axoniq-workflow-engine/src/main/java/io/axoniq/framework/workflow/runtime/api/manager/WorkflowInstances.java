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
package io.axoniq.framework.workflow.runtime.api.manager;

import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

/**
 * Represents a collection of workflow instances.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public interface WorkflowInstances extends WorkflowInstanceOperator {

    /**
     * Returns a publisher that asynchronously delivers the workflow instances in this batch.
     * <p>
     * The publisher must honor subscriber demand and must not perform workflow lookup before a subscriber requests
     * instances.
     *
     * @return publisher delivering the workflow instances in this batch
     */
    Flow.Publisher<WorkflowInstance> instances();

    /**
     * Returns the number of workflow instances in this collection.
     *
     * @return future completing with the number of workflow instances in this collection
     */
    CompletableFuture<Integer> size();

    /**
     * Represents a collection of at most one workflow instance.
     *
     */
    interface Single extends WorkflowInstances {

        /**
         * Resolves the single workflow instance in this collection.
         *
         * @return a future completing with the instance, or {@code null} when no instance matches
         */
        CompletableFuture<@Nullable WorkflowInstance> single();
    }
}
