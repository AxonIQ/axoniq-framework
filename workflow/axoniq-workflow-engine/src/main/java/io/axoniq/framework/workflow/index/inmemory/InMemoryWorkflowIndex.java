/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.inmemory;

import io.axoniq.framework.workflow.runtime.execution.SegmentKeyRange;
import io.axoniq.framework.workflow.runtime.execution.WaitAssociation;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WaitClosed;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WaitOpened;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtCleared;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtSet;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowEnded;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowStarted;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexRows;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link WorkflowIndex}, for tests and for an application that runs on a single node with
 * an in-memory token store.
 * <p>
 * The rows live in this instance, so they are lost with it and are not shared between nodes. A change stored in a
 * started {@link ProcessingContext} is applied when that context commits, like a token of the
 * {@link org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore}. A
 * change stored without one is applied at once.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
public class InMemoryWorkflowIndex implements WorkflowIndex {

    private final Map<String, ModuleRows> modules = new ConcurrentHashMap<>();
    private final boolean storesWithToken;

    /**
     * Creates an index that reports to {@link #storesWithToken() store with the token}: with the token store in the
     * same memory, no other node can hold the segment.
     */
    public InMemoryWorkflowIndex() {
        this(true);
    }

    /**
     * Creates an index that reports the given answer for {@link #storesWithToken()}.
     *
     * @param storesWithToken whether this index reports to store its rows with the token
     */
    public InMemoryWorkflowIndex(boolean storesWithToken) {
        this.storesWithToken = storesWithToken;
    }

    @Override
    public CompletableFuture<Boolean> isComplete(String module, @Nullable ProcessingContext context) {
        var rows = rowsOf(module);
        synchronized (rows) {
            return CompletableFuture.completedFuture(rows.complete);
        }
    }

    @Override
    public CompletableFuture<WorkflowIndexRows> load(String module,
                                                     Segment segment,
                                                     @Nullable ProcessingContext context) {
        var range = SegmentKeyRange.of(segment);
        var rows = rowsOf(module);
        synchronized (rows) {
            var running = new HashSet<String>();
            var wakesAt = new HashMap<String, Instant>();
            rows.workflows.forEach((workflowId, wakes) -> {
                if (range.contains(SegmentKeyRange.keyOf(workflowId))) {
                    running.add(workflowId);
                    if (wakes != null) {
                        wakesAt.put(workflowId, wakes);
                    }
                }
            });
            var waits = rows.waits.values()
                                  .stream()
                                  .filter(wait -> range.contains(SegmentKeyRange.keyOf(wait.workflowId())))
                                  .toList();
            return CompletableFuture.completedFuture(new WorkflowIndexRows(running, wakesAt, waits));
        }
    }

    @Override
    public CompletableFuture<Void> store(String module,
                                         WorkflowIndexChanges changes,
                                         @Nullable ProcessingContext context) {
        if (context != null && context.isStarted()) {
            // Applied with the commit, so a transaction that rolls back leaves no row behind.
            context.runOnAfterCommit(committed -> apply(module, changes));
        } else {
            apply(module, changes);
        }
        return CompletableFuture.completedFuture(null);
    }

    private void apply(String module, WorkflowIndexChanges changes) {
        var rows = rowsOf(module);
        synchronized (rows) {
            changes.changes().forEach(rows::apply);
            if (changes.complete()) {
                rows.complete = true;
            }
        }
    }

    @Override
    public boolean storesWithToken() {
        return storesWithToken;
    }

    private ModuleRows rowsOf(String module) {
        return modules.computeIfAbsent(module, name -> new ModuleRows());
    }

    /**
     * The rows of one workflow module. Guarded by its own monitor.
     */
    private static final class ModuleRows {

        // The running workflows, each with the time it wakes at, or null when it is not hibernated.
        private final Map<String, @Nullable Instant> workflows = new HashMap<>();
        private final Map<WaitKey, WaitAssociation> waits = new LinkedHashMap<>();
        private boolean complete;

        private void apply(WorkflowIndexChange change) {
            switch (change) {
                case WorkflowStarted started -> {
                    if (!workflows.containsKey(started.workflowId())) {
                        workflows.put(started.workflowId(), null);
                    }
                }
                case WorkflowEnded ended -> {
                    workflows.remove(ended.workflowId());
                    waits.keySet().removeIf(key -> key.workflowId().equals(ended.workflowId()));
                }
                case WaitOpened opened -> waits.put(
                        new WaitKey(opened.workflowId(), opened.association().stepName()), opened.association()
                );
                case WaitClosed closed -> waits.remove(new WaitKey(closed.workflowId(), closed.stepName()));
                case WakesAtSet set -> workflows.put(set.workflowId(), set.wakesAt());
                case WakesAtCleared cleared -> {
                    if (workflows.containsKey(cleared.workflowId())) {
                        workflows.put(cleared.workflowId(), null);
                    }
                }
            }
        }
    }

    private record WaitKey(String workflowId, String stepName) {

    }
}
