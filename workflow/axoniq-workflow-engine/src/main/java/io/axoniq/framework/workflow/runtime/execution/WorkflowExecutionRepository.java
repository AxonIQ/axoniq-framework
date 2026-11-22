/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

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
     * Returns all stored workflow executions owned by the given segment: those whose workflow identifier the segment
     * {@link Segment#matches(Object) matches}. An event processor handles an event once per segment it holds, and acts
     * on the executions of that segment only, so this lookup is on the path of every event broadcast to the workflows
     * of a segment.
     * <p>
     * The default implementation filters all stored executions. Implementations should override it to visit only the
     * executions of the given segment, so the cost of a lookup does not grow with the executions of other segments.
     *
     * @param segment the segment owning the executions, or {@code null} for all executions, as outside a segmented
     *                processor every execution is in scope
     * @return an unmodifiable collection of the workflow executions owned by the segment
     * @since 5.5.0
     */
    default Set<WorkflowExecution> findAllOwnedBy(@Nullable Segment segment) {
        return findAll(execution -> WorkflowSegmentOwnership.ownedBy(segment, execution.workflowId()));
    }

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
    @Nullable
    WorkflowExecution remove(String workflowId);

    /**
     * Removes the given workflow execution, but only while it is the one stored for the given identifier. An execution
     * that left the engine and was replaced by another execution of the same workflow, for example one restored by a
     * later claim of its segment or one woken from hibernation, thereby never removes its replacement.
     * <p>
     * The default implementation checks and removes in two steps. Implementations should override it to do both
     * atomically.
     *
     * @param workflowId the identifier of the workflow
     * @param execution  the execution to remove
     * @return {@code true} when the given execution was stored and is removed, {@code false} otherwise
     * @since 5.5.0
     */
    default boolean remove(String workflowId, WorkflowExecution execution) {
        if (findById(workflowId).orElse(null) != execution) {
            return false;
        }
        removeAll(candidate -> candidate == execution);
        return true;
    }

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
