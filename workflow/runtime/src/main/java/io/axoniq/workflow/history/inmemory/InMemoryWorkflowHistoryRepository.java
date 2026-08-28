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
package io.axoniq.workflow.history.inmemory;

import io.axoniq.workflow.history.api.WorkflowHistory;
import io.axoniq.workflow.history.api.WorkflowHistoryRepository;
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

    @Override
    public List<WorkflowHistory> findAll() {
        return List.copyOf(workflowHistoryMap.values());
    }

    @Override
    public Optional<WorkflowHistory> findById(String workflowId) {
        return Optional.ofNullable(workflowHistoryMap.get(workflowId));
    }

    @Override
    public void save(WorkflowHistory workflowHistory) {
        workflowHistoryMap.put(workflowHistory.workflowId(), workflowHistory);
    }

    @Override
    public void clear() {
        workflowHistoryMap.clear();
    }
}
