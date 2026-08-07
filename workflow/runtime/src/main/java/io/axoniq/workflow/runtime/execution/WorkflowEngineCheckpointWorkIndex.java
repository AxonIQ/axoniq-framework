/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 * you may not use this file except in compliance with the License.
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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Index of workflow executions that may still make checkpoint advancement unsafe.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
final class WorkflowEngineCheckpointWorkIndex {

    private final Set<String> unsafeWorkflowIds = ConcurrentHashMap.newKeySet();

    boolean hasUnsafeCheckpointWork() {
        return !unsafeWorkflowIds.isEmpty();
    }

    @Nonnull
    Set<String> unsafeWorkflowIds() {
        return Set.copyOf(unsafeWorkflowIds);
    }

    void markUnsafe(@Nonnull String workflowId) {
        unsafeWorkflowIds.add(workflowId);
    }

    void markSafe(@Nonnull String workflowId) {
        unsafeWorkflowIds.remove(workflowId);
    }

    /**
     * Registers a workflow as a source of checkpoint-work state transitions.
     *
     * @param workflowId       identifier of the workflow whose checkpoint-work state is indexed
     * @param listenerRegistrar operation that installs the state listener on the workflow execution
     */
    void register(
            @Nonnull String workflowId,
            @Nonnull Consumer<WorkflowExecution.CheckpointWorkStateListener> listenerRegistrar
    ) {
        listenerRegistrar.accept(new WorkflowExecution.CheckpointWorkStateListener() {
            @Override
            public void onCheckpointWorkBecameUnsafe() {
                markUnsafe(workflowId);
            }

            @Override
            public void onCheckpointWorkBecameSafe() {
                markSafe(workflowId);
            }
        });
    }

    void clear() {
        unsafeWorkflowIds.clear();
    }
}
