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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.DescribableComponent;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Repository for retrieving workflow executions, storing new ones, and removing them.
 * <p>
 * Each workflow execution is identified by a unique workflow identifier and is represented as a
 * {@link WorkflowExecution} bundling configuration, context, and execution state.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public interface WorkflowExecutionRepository extends DescribableComponent {

    /**
     * Finds a workflow execution by its identifier.
     *
     * @param workflowId the workflow identifier
     * @return an {@link Optional} containing the workflow execution, or empty if not found
     */
    Optional<WorkflowExecution> findById(String workflowId);

    /**
     * Returns all stored workflow executions, with at most one execution per workflow identifier.
     *
     * @return an unmodifiable collection of all workflow executions
     */
    default Set<WorkflowExecution> findAll() {
        return findAll(e -> true);
    }

    /**
     * Returns all stored workflow executions whose state matches a query.
     *
     * @param query state criteria to apply
     * @return a future completing with an unmodifiable collection of matching workflow executions
     */
    CompletableFuture<Set<WorkflowExecution>> findAll(WorkflowStateQuery query);

    /**
     * Returns all stored workflow executions matching the given predicate, with at most one execution per workflow
     * identifier.
     *
     * @param predicate the predicate to match workflow executions against
     * @return an unmodifiable collection of all workflow executions
     */
    Set<WorkflowExecution> findAll(Predicate<WorkflowExecution> predicate);

    /**
     * Atomically stores a new workflow execution if no execution exists for the given workflow identifier. If an
     * execution already exists, returns the existing one without invoking the factory.
     *
     * @param factory the factory to create a new workflow execution if absent
     * @return the existing or newly created workflow execution
     */
    WorkflowExecution save(String workflowId, Supplier<WorkflowExecution> factory);

    /**
     * Removes the workflow execution associated with the given identifier.
     *
     * @param workflowId the identifier of the workflow to remove
     * @return the removed {@link WorkflowExecution}, or {@code null} if no execution was found
     */
    WorkflowExecution remove(String workflowId);

    /**
     * Removes all workflow executions matching the given predicate.
     *
     * @param predicate the predicate to match workflow executions against
     */
    void removeAll(Predicate<WorkflowExecution> predicate);

    /**
     * Removes all stored workflow executions.
     */
    void clear();
}
