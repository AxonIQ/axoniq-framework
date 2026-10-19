/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.history.inmemory;

import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.history.api.WorkflowHistoryRepository;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.query.utils.WorkflowStateQueryMatcher;
import org.axonframework.common.annotation.Internal;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link WorkflowHistoryRepository}.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class InMemoryWorkflowHistoryRepository implements MutableWorkflowHistoryRepository {

    private final ConcurrentHashMap<String, WorkflowHistory> workflowHistoryMap = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<List<WorkflowHistory>> findAll(WorkflowStateQuery query) {
        return CompletableFuture.completedFuture(workflowHistoryMap.values().stream()
                                                                   .filter(history -> WorkflowStateQueryMatcher.matches(
                                                                           query, history.state()
                                                                   ))
                                                                   .toList());
    }

    @Override
    public CompletableFuture<Optional<WorkflowHistory>> findById(String workflowId) {
        return CompletableFuture.completedFuture(Optional.ofNullable(workflowHistoryMap.get(workflowId)));
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
