/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution.CheckpointWorkStateListener;
import org.axonframework.common.annotation.Internal;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Index containing the identifiers of {@link WorkflowExecution WorkflowExecutions} that still may make checkpoint
 * advancement unsafe.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal final class UnsafeCheckpointWorkIndex {

    private final Set<String> unsafeWorkflowIds = ConcurrentHashMap.newKeySet();

    boolean hasUnsafeCheckpointWork() {
        return !unsafeWorkflowIds.isEmpty();
    }

    Set<String> unsafeWorkflowIds() {
        return Set.copyOf(unsafeWorkflowIds);
    }

    void markSafe(String workflowId) {
        unsafeWorkflowIds.remove(workflowId);
    }

    void markUnsafe(String workflowId) {
        unsafeWorkflowIds.add(workflowId);
    }

    /**
     * Registers a workflow as a source of checkpoint-work state transitions.
     *
     * @param workflowId        identifier of the workflow whose checkpoint-work state is indexed
     * @param listenerRegistrar operation that installs the state listener on the workflow execution
     */
    void register(String workflowId,
                  Consumer<CheckpointWorkStateListener> listenerRegistrar) {
        listenerRegistrar.accept(new CheckpointWorkStateListener() {
            @Override
            public void onMarkedUnsafe() {
                markUnsafe(workflowId);
            }

            @Override
            public void onMarkedSafe() {
                markSafe(workflowId);
            }
        });
    }

    void clear() {
        unsafeWorkflowIds.clear();
    }
}
