/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.api;

import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.DescribableComponent;

import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Repository for retrieving workflow handles.
 * <p>
 * Each workflow instance is identified by a unique workflow identifier and is represented
 * as a {@link WorkflowHandle} bundling configuration, context, and execution state.
 */
public interface WorkflowRepository extends DescribableComponent {

    /**
     * Finds a workflow handle by its identifier.
     *
     * @param workflowId the workflow identifier
     * @return the workflow handle, or {@code null} if not found
     */
    WorkflowHandle findById(@Nonnull String workflowId);

    /**
     * Returns all stored workflow handles.
     *
     * @return an unmodifiable collection of all workflow handles
     */
    @Nonnull
    Collection<WorkflowHandle> findAll();

    /**
     * Returns all stored workflow handles as an unmodifiable map keyed by workflow identifier.
     *
     * @return an unmodifiable map of workflow identifiers to workflow handles
     */
    @Nonnull
    Map<String, WorkflowHandle> findAllAsMap();

    /**
     * Cancels the workflow instance with the given identifier.
     * The workflow must be in a non-terminal state.
     *
     * @param workflowId the workflow identifier to cancel
     * @throws UnsupportedOperationException until implemented
     */
    default void cancel(@Nonnull String workflowId) {
        throw new UnsupportedOperationException("cancel(workflowId) is not yet implemented");
    }

    /**
     * Cancels all workflow instances matching the given predicate.
     *
     * @param predicate the condition to match workflow instances for cancellation
     * @throws UnsupportedOperationException until implemented
     */
    default void cancelIf(@Nonnull Predicate<WorkflowHandle> predicate) {
        throw new UnsupportedOperationException("cancelIf(predicate) is not yet implemented");
    }
}
