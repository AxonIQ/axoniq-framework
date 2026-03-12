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
package io.axoniq.workflow.runtime.engine.history;

import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link WorkflowHistoryRepository}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class InMemoryWorkflowHistoryRepository implements MutableWorkflowHistoryRepository {

    private final ConcurrentHashMap<String, WorkflowHistory> workflowHistoryMap = new ConcurrentHashMap<>();

    @Nonnull
    @Override
    public List<WorkflowHistory> findAll() {
        return List.copyOf(workflowHistoryMap.values());
    }

    @Nonnull
    @Override
    public Optional<WorkflowHistory> findById(@Nonnull String workflowIds) {
        return Optional.ofNullable(workflowHistoryMap.get(workflowIds));
    }

    @Override
    public void save(@Nonnull WorkflowHistory workflowHistory) {
        workflowHistoryMap.put(workflowHistory.workflowId(), workflowHistory);
    }

    @Override
    public void clear() {
        workflowHistoryMap.clear();
    }
}
