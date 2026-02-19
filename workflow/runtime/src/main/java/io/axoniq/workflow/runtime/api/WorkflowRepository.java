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
import java.util.Optional;

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
     * @return an {@link Optional} containing the workflow handle, or empty if not found
     */
    @Nonnull
    Optional<WorkflowHandle> findById(@Nonnull String workflowId);

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
}
