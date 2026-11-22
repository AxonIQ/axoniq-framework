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
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Index containing the identifiers of {@link WorkflowExecution WorkflowExecutions} that still may make checkpoint
 * advancement unsafe.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal final class UnsafeCheckpointWorkIndex {

    private final SegmentBucketedMap<Boolean> unsafeWorkflowIds = new SegmentBucketedMap<>();

    /**
     * Returns the identifiers of the unsafe workflows owned by the given segment, without visiting those of other
     * segments: a segment checks them on every checkpoint, while the index holds the workflows of every segment on this
     * node.
     *
     * @param segment the segment owning the workflows
     * @return a snapshot of the identifiers of the unsafe workflows owned by the segment
     */
    Set<String> unsafeWorkflowIdsOf(Segment segment) {
        return Set.copyOf(unsafeWorkflowIds.entriesOf(segment).map(Map.Entry::getKey).toList());
    }

    void markSafe(String workflowId) {
        unsafeWorkflowIds.remove(workflowId);
    }

    void markUnsafe(String workflowId) {
        unsafeWorkflowIds.put(workflowId, Boolean.TRUE);
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
