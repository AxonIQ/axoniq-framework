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
import java.util.Set;
import java.util.function.Supplier;

/**
 * Repository for retrieving workflow handles.
 * <p>
 * Each workflow instance is identified by a unique workflow identifier and is represented
 * as a {@link WorkflowExecution} bundling configuration, context, and execution state.
 */
public interface WorkflowExecutionRepository extends DescribableComponent {

    /**
     * Finds a workflow handle by its identifier.
     *
     * @param workflowId the workflow identifier
     * @return an {@link Optional} containing the workflow handle, or empty if not found
     */
    @Nonnull
    Optional<WorkflowExecution> findById(@Nonnull String workflowId);

    /**
     * Returns all stored workflow handles.
     *
     * @return an unmodifiable collection of all workflow handles
     */
    @Nonnull
    Set<WorkflowExecution> findAll();

    /**
     * Atomically stores a new workflow handle if no handle exists for the given workflow identifier.
     * If a handle already exists, returns the existing one without invoking the factory.
     *
     * @param factory    the factory to create a new workflow handle if absent
     * @return the existing or newly created workflow handle
     */
    @Nonnull
    WorkflowExecution save(@Nonnull Supplier<WorkflowExecution> factory);

    /**
     * Remove workflow stored workflow handle.
     */
    WorkflowExecution remove(@Nonnull String workflowId);

    /**
     * Removes all stored workflow handles.
     */
    void clear();
}
