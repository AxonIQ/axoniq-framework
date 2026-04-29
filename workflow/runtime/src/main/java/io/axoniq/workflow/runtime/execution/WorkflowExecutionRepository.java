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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.DescribableComponent;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Repository for retrieving workflow instances.
 * <p>
 * Each workflow instance is identified by a unique workflow identifier and is represented
 * as a {@link WorkflowInstance} bundling configuration, context, and execution state.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@Internal
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
    WorkflowExecution save(@Nonnull String workflowId, @Nonnull Supplier<WorkflowExecution> factory);

    /**
     * Removes the workflow handle associated with the given identifier.
     *
     * @param workflowId the identifier of the workflow to remove
     * @return the removed {@link WorkflowExecution}, or {@code null} if no handle was found
     */
    WorkflowExecution remove(@Nonnull String workflowId);

    /**
     * Removes all stored workflow handles.
     */
    void clear();
}
