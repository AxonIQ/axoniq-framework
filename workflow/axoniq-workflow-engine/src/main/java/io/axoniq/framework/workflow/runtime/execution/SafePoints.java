/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution.CheckpointWorkStateListener;
import io.axoniq.framework.workflow.runtime.execution.LiveWorkflows.InMemory;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * The safe points of the workflows in memory of one workflow module: the entries whose execution last reported work in
 * progress that makes a checkpoint of its segment unsafe.
 * <p>
 * An execution reports its safe points through the listener it gets as it enters memory, see
 * {@link #listenerOf(InMemory)}. That listener only marks the entry of the execution, never the workflow: an execution
 * that left memory, for example because it hibernated or its segment was released, keeps unwinding for a moment, and
 * must not mark the workflow safe or unsafe on behalf of the next execution of that workflow. Two rules keep it so:
 * <ul>
 *     <li>marking unsafe checks, while it holds the bucket of the unsafe entries, that the entry is still the entry of
 *     its workflow;</li>
 *     <li>the {@link LiveWorkflows} registry removes an entry from its entries before it {@link #forget(InMemory)
 *     forgets} the mark of that entry.</li>
 * </ul>
 * So a mark either precedes the removal of its mark and is removed by it, or follows the removal of the entry and is
 * refused, and marking safe only removes the entry's own mark, never that of a successor.
 * <p>
 * The marks are kept per segment, so a checkpoint of a segment visits only the unsafe entries of that segment, see
 * {@link #unsafeOf(Segment)}, while this node holds the workflows of every segment it claimed. A segment token only
 * covers the workflows that segment owns, so executions of other segments must not hold it back.
 * <p>
 * The {@link LiveWorkflows} registry owns it, hands the listener to each execution that enters memory, and forgets the
 * mark of each entry that leaves; the {@link WorkflowEngine} reads it as it checkpoints a segment.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
final class SafePoints {

    private final SegmentBucketedMap<InMemory> unsafe = new SegmentBucketedMap<>();
    private final Predicate<InMemory> isEntryOfItsWorkflow;

    /**
     * Creates the safe points of the entries of a registry.
     *
     * @param isEntryOfItsWorkflow tells whether an entry is still the entry of its workflow in the registry
     */
    SafePoints(Predicate<InMemory> isEntryOfItsWorkflow) {
        this.isEntryOfItsWorkflow = Objects.requireNonNull(isEntryOfItsWorkflow,
                                                           "The entry predicate must not be null.");
    }

    /**
     * Returns the listener through which the execution of the given entry reports its safe points. Marking unsafe is
     * refused once the entry is no longer the entry of its workflow, see the class documentation.
     *
     * @param entry the entry of an execution that enters memory
     * @return the listener of the execution of the entry
     */
    CheckpointWorkStateListener listenerOf(InMemory entry) {
        var workflowId = entry.workflowId();
        return new CheckpointWorkStateListener() {
            @Override
            public void onMarkedUnsafe() {
                unsafe.compute(workflowId, (id, current) -> isEntryOfItsWorkflow.test(entry) ? entry : current);
            }

            @Override
            public void onMarkedSafe() {
                unsafe.remove(workflowId, entry);
            }
        };
    }

    /**
     * Returns the entries of the workflows of the given segment whose execution last reported work in progress that
     * makes a checkpoint unsafe, without visiting those of other segments. An entry that is leaving memory is left out,
     * as it no longer is the entry of its workflow.
     *
     * @param segment the segment owning the workflows
     * @return a snapshot of the entries of the segment's unsafe workflows in memory
     */
    List<InMemory> unsafeOf(Segment segment) {
        return unsafe.entriesOf(segment)
                     .map(Map.Entry::getValue)
                     .filter(isEntryOfItsWorkflow)
                     .toList();
    }

    /**
     * Marks the given entry safe, as its execution turned out to have no work in progress that makes a checkpoint
     * unsafe, unless another entry of its workflow replaced it meanwhile.
     *
     * @param entry the entry to mark safe
     */
    void markSafe(InMemory entry) {
        unsafe.remove(entry.workflowId(), entry);
    }

    /**
     * Forgets the mark of the given entry, which left memory: work its execution still has in progress no longer holds
     * back a checkpoint of its segment. The caller removed the entry from the entries first, see the class
     * documentation.
     *
     * @param entry the entry that left memory
     */
    void forget(InMemory entry) {
        unsafe.remove(entry.workflowId(), entry);
    }

    /**
     * Forgets every mark, as the engine shuts down.
     */
    void clear() {
        unsafe.clear();
    }
}
