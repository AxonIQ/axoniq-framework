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

import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Provides the outside-in API for locating workflow instances and requesting cancellation from application or
 * administrative code. Use {@link #findOne(WorkflowStateQuery)} when the query must select at most one instance, or
 * {@link #findMany(WorkflowStateQuery)} to operate on every matching instance.
 * <p>
 * Cancellation requests are asynchronous and affect live workflow executions only. For example, to request cancellation
 * of a workflow identified by its workflow ID:
 * <pre>{@code
 * workflowManager.findOne(WorkflowStateQuery.byWorkflowId(workflowId))
 *                .requestWorkflowCancellation(null);
 * }</pre>
 * The returned future completes after the cancellation event is durable and the workflow body has been woken. Use the
 * workflow execution context to initiate cancellation from inside a workflow definition.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public interface WorkflowManager {

    /**
     * Finds the single workflow instance matching a query.
     * <p>
     * The returned result evaluates the query when {@link WorkflowInstances.Single#singleState()} is invoked. That
     * operation completes with {@code null} when no instance matches. When more than one instance matches, calling
     * {@link CompletableFuture#join()} throws a {@link CompletionException} whose cause is a
     * {@link NonUniqueWorkflowInstanceMatchException}.
     * <p>
     * Use a workflow ID when it uniquely identifies the instance to resolve:
     * <pre>{@code
     * workflowManager.findOne(WorkflowStateQuery.byWorkflowId(workflowId))
     *                .singleState()
     *                .thenAccept(state -> {
     *                    if (state != null) {
     *                        inspect(state);
     *                    }
     *                });
     * }</pre>
     *
     * @param query criteria used to select the workflow instance
     * @return lazy result for the single matching workflow instance
     */
    WorkflowInstances.Single findOne(WorkflowStateQuery query);

    /**
     * Finds every workflow instance matching a query.
     *
     * @param query criteria used to select workflow instances
     * @return lazy handle for the matching workflow instances
     */
    WorkflowInstances findMany(WorkflowStateQuery query);
}
