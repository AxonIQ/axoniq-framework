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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.history.api;

import jakarta.annotation.Nonnull;

import java.util.List;
import java.util.Optional;

/**
 * Repository to access workflow history entries.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public interface WorkflowHistoryRepository {

    /**
     * Returns all stored workflow history entries.
     *
     * @return an unmodifiable collection of all workflow history entries.
     */
    @Nonnull
    List<WorkflowHistory> findAll();


    /**
     * Returns workflow history by given workflow id.
     *
     * @return a workflow history or null, if no history exists for the given workflow id.
     */
    @Nonnull
    Optional<WorkflowHistory> findById(@Nonnull String workflowIds);
}
