/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import io.axoniq.framework.workflow.runtime.execution.LiveWorkflows.Hibernated;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;

/**
 * Finds the workflows of a segment a broadcast event goes to: every workflow of the segment that is in memory, and
 * every hibernated workflow of the segment with an open wait that may match the event.
 * <p>
 * It keeps the open waits of the hibernated workflows as rows of a {@link WaitAssociationIndex}, so the lookup visits
 * only the hibernated workflows that may wait for the event, and lists the executions in memory from the
 * {@link WorkflowExecutionRepository}. The {@link LiveWorkflows} registry owns it, and writes the rows of a workflow
 * under the lock of that workflow, as the rows are keyed by its identifier.
 * <p>
 * <b>The guarantee.</b> The lookup never misses a workflow that hibernates, wakes, or hibernates and wakes while it
 * runs. Listing and matching at two moments could: a workflow listed before it hibernates and matched once it woke
 * again is in neither result, and as its wake restores it from its own events, it would never see the broadcast event.
 * So:
 * <ul>
 *     <li>every move of a workflow between memory and hibernation, which moves it between the repository and the
 *     rows, runs as one {@link #swap(Supplier) swap}, under the write lock of a {@link StampedLock}, the swap
 *     lock;</li>
 *     <li>the lookup runs on an optimistic stamp of that lock, and when a swap ran meanwhile, runs once more under its
 *     read lock, which no swap interleaves.</li>
 * </ul>
 * The result is the state of the repository and the rows at one instant between swaps, within the lookup. At such an
 * instant, a workflow that stays live is found where it is: in the repository while its execution is in memory, and by
 * the rows of its open waits while it is hibernated. One that hibernated without a wait for an event has no rows, and
 * only an event of its own wakes it, which is delivered by its identifier, not broadcast.
 * <p>
 * The transitions that run without a swap only add a workflow to one side or remove it from one, so a lookup may see
 * them half done: as an execution leaves memory, the workflow is in neither while its entry still stands, and as a
 * claim adopts a hibernated workflow, its rows are there a moment before its entry. For these transitions, "not live
 * yet" or "no longer live" is a correct answer, as they are no moves of a workflow that stays live: a workflow enters
 * or leaves memory this way as it starts or ends, or as a claim of its segment starts or ends and restores, adopts,
 * stops or drops it, and a claim handles the events of its segment from the position it resumes from, so an event
 * routed while the claim changes is handled again by the next claim.
 * <p>
 * <b>Locks.</b> The write lock of the swap lock is a leaf of the locks of the engine: under it, a swap only touches the
 * entries of the registry, the rows and the repository; nothing takes the lock of a workflow, of a claim or of the
 * {@link RunningWorkflowsProjection projection}, and nothing runs or loads a workflow. Only a repeated lookup holds the
 * read lock, and takes no other lock of the registry under it. A lookup never makes its first attempt under the read
 * lock: a repository may, while it lists, wait for a workflow to hibernate or wake on another thread, and a read lock
 * held by the listing thread would keep that swap from ever finishing. When a swap holds the write lock as the lookup
 * starts, the lookup first waits for that swap to finish, and then makes its attempt on an optimistic stamp.
 * <p>
 * <b>Cost.</b> One listing of the executions of the segment in memory, and one match of the rows, which finds the keyed
 * rows by the values the event carries and checks only those, the rows of the event's name without key, and the rows
 * without a name. The swap lock adds a volatile read and a validation of the stamp. Without hibernation, no swap ever
 * runs, so the stamp never fails. Swaps of all workflows of the module are serialized, which costs little, as a swap
 * only updates the entries, the rows and the repository; the rows of a hibernation are parsed before, see
 * {@link #rowsOf(String, HibernationHint)}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
final class BroadcastAddressees {

    private final WorkflowExecutionRepository repository;
    private final WaitAssociationIndex waits = new WaitAssociationIndex();
    private final StampedLock swaps = new StampedLock();

    /**
     * Creates the lookup of the addressees of broadcast events, which lists the executions in memory from the given
     * repository.
     *
     * @param repository the store of the executions in memory
     */
    BroadcastAddressees(WorkflowExecutionRepository repository) {
        this.repository = Objects.requireNonNull(repository, "The WorkflowExecutionRepository must not be null.");
    }

    /**
     * Returns the workflows of the given segment a broadcast event goes to: every execution in memory, and every
     * hibernated workflow with an open wait that may match the event, as at one instant between swaps, see the class
     * documentation.
     *
     * @param segment the segment handling the event, or {@code null} outside a segmented processor
     * @param event   the broadcast event
     * @param context the context of the event
     * @return the identifiers of the addressees
     */
    Set<String> of(@Nullable Segment segment, EventMessage event, ProcessingContext context) {
        long stamp;
        while ((stamp = swaps.tryOptimisticRead()) == 0L) {
            // A swap is in progress: wait for it, but attempt the lookup without holding the read lock.
            swaps.unlockRead(swaps.readLock());
        }
        var addressees = lookUp(segment, event, context);
        if (swaps.validate(stamp)) {
            return addressees;
        }
        // A swap ran meanwhile: look up once more, while no swap runs.
        long readStamp = swaps.readLock();
        try {
            return lookUp(segment, event, context);
        } finally {
            swaps.unlockRead(readStamp);
        }
    }

    private Set<String> lookUp(@Nullable Segment segment, EventMessage event, ProcessingContext context) {
        var workflowIds = new LinkedHashSet<String>();
        repository.findAllOwnedBy(segment).forEach(execution -> workflowIds.add(execution.workflowId()));
        waits.match(event, context)
             .stream()
             .filter(workflowId -> WorkflowSegmentOwnership.ownedBy(segment, workflowId))
             .forEach(workflowIds::add);
        return workflowIds;
    }

    /**
     * Returns whether one of the open waits of the given hibernated workflow may match the given event, so a broadcast
     * event that finds the workflow hibernated as it is delivered wakes it only when it may wait for the event.
     *
     * @param workflowId the identifier of the hibernated workflow
     * @param event      the broadcast event
     * @param context    the context of the event
     * @return {@code true} when the workflow may wait for the event
     */
    boolean awaits(String workflowId, EventMessage event, ProcessingContext context) {
        return waits.matches(workflowId, event, context);
    }

    /**
     * Runs the given move of a workflow between memory and hibernation, which moves it between the repository and the
     * rows, as one swap: under the write lock of the swap lock, so no lookup sees it half done. The move only touches
     * the entries of the registry, the rows and the repository, see the class documentation.
     *
     * @param move the move of a workflow between the repository and the rows
     * @param <T>  the type of the result of the move
     * @return the result of the move
     */
    <T> T swap(Supplier<T> move) {
        long stamp = swaps.writeLock();
        try {
            return move.get();
        } finally {
            swaps.unlockWrite(stamp);
        }
    }

    /**
     * Parses the open waits of the given hint into the rows of a hibernation of the given workflow, so a swap adds them
     * without parsing them under the swap lock.
     *
     * @param workflowId the identifier of the workflow
     * @param hint       the open waits and nearest deadline the workflow hibernates with
     * @return the rows of the open waits
     */
    List<WaitAssociationIndex.Row> rowsOf(String workflowId, HibernationHint hint) {
        return hint.waits().stream()
                   .map(wait -> waits.rowOf(
                           workflowId,
                           wait.stepName(),
                           wait.eventName() == null ? null : new QualifiedName(wait.eventName()),
                           wait.associations()
                   ))
                   .toList();
    }

    /**
     * Adds the rows of the open waits of the given hibernation. The caller holds the lock of the workflow.
     *
     * @param hibernated the hibernation whose rows to add
     */
    void add(Hibernated hibernated) {
        hibernated.rows().forEach(waits::add);
    }

    /**
     * Removes the rows of the open waits of the given workflow. The caller holds the lock of the workflow.
     *
     * @param workflowId the identifier of the workflow whose rows to remove
     */
    void remove(String workflowId) {
        waits.removeWorkflow(workflowId);
    }

    /**
     * Removes every row, as the engine shuts down.
     */
    void clear() {
        waits.clear();
    }
}
