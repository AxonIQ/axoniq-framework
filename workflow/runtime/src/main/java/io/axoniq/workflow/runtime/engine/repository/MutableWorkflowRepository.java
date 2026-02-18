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
package io.axoniq.workflow.runtime.engine.repository;

import io.axoniq.workflow.runtime.api.WorkflowHandle;
import io.axoniq.workflow.runtime.api.WorkflowRepository;
import jakarta.annotation.Nonnull;

import java.util.function.Function;

/**
 * Internal extension of {@link WorkflowRepository} that adds mutation operations
 * used by the workflow engine.
 */
public interface MutableWorkflowRepository extends WorkflowRepository {

    /**
     * Atomically stores a new workflow handle if no handle exists for the given workflow identifier.
     * If a handle already exists, returns the existing one without invoking the factory.
     *
     * @param workflowId the workflow identifier
     * @param factory    the factory to create a new workflow handle if absent
     * @return the existing or newly created workflow handle
     */
    @Nonnull
    WorkflowHandle storeIfAbsent(@Nonnull String workflowId, @Nonnull Function<String, WorkflowHandle> factory);

    /**
     * Removes all stored workflow handles.
     */
    void clear();
}
