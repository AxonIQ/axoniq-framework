/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.manager;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Represents a lazy collection of {@link WorkflowInstance workflow instances} matching a query. The collection can
 * contain both live workflow executions and historic workflow instances; obtain the individual instances through
 * {@link #instances()}. Obtain this result from {@link WorkflowManager#findMany(WorkflowStateQuery)}. Cancellation
 * requests made through this collection operate only on the matching live executions; historic instances are not
 * modified.
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
    Publisher<WorkflowInstance> instances();

    /**
     * Returns the number of workflow instances in this collection.
     *
     * @return future completing with the number of workflow instances in this collection
     */
    CompletableFuture<Integer> size();

    /**
     * Represents a query result containing at most one {@link WorkflowInstance workflow instance}. Obtain this result
     * from {@link WorkflowManager#findOne(WorkflowStateQuery)}.
     */
    interface Single extends WorkflowInstances {

        /**
         * Resolves the detached state of the single workflow instance in this collection.
         *
         * @return a future completing with the detached state, or {@code null} when no instance matches. If more than
         * one instance matches, {@link CompletableFuture#join()} throws a {@link CompletionException} whose cause is a
         * {@link NonUniqueWorkflowInstanceMatchException}.
         */
        CompletableFuture<@Nullable WorkflowState> singleState();
    }
}
