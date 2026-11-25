/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WaitClosed;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WaitOpened;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtCleared;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtSet;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowEnded;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowStarted;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.runtime.util.ProcessingContextUtils;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GapAwareTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingTokenUtils;
import org.axonframework.messaging.eventhandling.processing.streaming.token.WrappedToken;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Keeps the {@link WorkflowIndex} of a {@link WorkflowEngine} current, and answers a segment claim from it.
 * <p>
 * While the engine handles the events of the workflows a segment owns, this support notes what changes in the rows: a
 * workflow started or ended, a wait opened or closed, a workflow hibernated or woke. Each time the processor stores a
 * token, it stores the noted changes that token covers, in the same call and, for an index that
 * {@link WorkflowIndex#storesWithToken() stores with the token}, in the same transaction. The token is therefore the
 * position of the rows.
 * <p>
 * A claim reads the rows of its segment, and then the tail: the lifecycle and wait events after the token the segment
 * resumes from. Rows plus tail are the running workflows and the open waits as of the claim, which is what sourcing the
 * {@link RunningWorkflows} and the {@link WaitAssociations} in full gives. A change is noted again when the processor
 * replays that tail, and storing it again has no further effect.
 * <p>
 * Like those two lists, the rows hold every workflow and wait a segment owns, also those of another workflow module on
 * the same event store. A claim skips the workflows of another module, as it does without an index.
 * <p>
 * The {@link WorkflowEngine} creates it when a {@link WorkflowIndex} is configured, the way it creates its
 * {@link WorkflowEngineHibernationSupport}.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
final class WorkflowEngineIndexSupport {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngineIndexSupport.class);

    private final WorkflowIndex index;
    private final String module;
    private final UnitOfWorkFactory unitOfWorkFactory;
    // The noted changes of each claimed segment, by segment id.
    private final Map<Integer, PendingChanges> pending = new ConcurrentHashMap<>();

    /**
     * Creates the index support of an engine.
     *
     * @param index             the index to keep current and to claim from
     * @param module            the name of the workflow module, which keeps its rows apart from those of other modules
     * @param unitOfWorkFactory creates the unit of work the tail of a claim is read in
     */
    WorkflowEngineIndexSupport(WorkflowIndex index, String module, UnitOfWorkFactory unitOfWorkFactory) {
        this.index = Objects.requireNonNull(index, "The WorkflowIndex must not be null.");
        this.module = Objects.requireNonNull(module, "The module name must not be null.");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "The UnitOfWorkFactory must not be null.");
    }

    /**
     * Notes what the given event of a workflow changes in the rows of its segment.
     * <p>
     * Every lifecycle and wait event of a workflow the segment owns is noted, also when this engine does not hold the
     * workflow: the restore of a workflow can fail, and its row is what lets a later claim find it again.
     *
     * @param segment the segment that handles the event, or {@code null} outside a segmented processor
     * @param token   the position of the event, or {@code null} when it is not known
     * @param event   an event that carries a workflow id and that the segment owns
     */
    void record(@Nullable Segment segment, @Nullable TrackingToken token, EventMessage event) {
        if (segment == null) {
            return;
        }
        var metadata = event.metadata();
        var changes = new ArrayList<WorkflowIndexChange>(2);
        if (MetadataUtils.getWorkflowStatus(metadata)
                         .filter(status -> status == WorkflowStatus.STARTED)
                         .isPresent()) {
            changes.add(new WorkflowStarted(MetadataUtils.getWorkflowId(metadata)));
        }
        switch (WaitAssociationChange.of(event)) {
            case WaitAssociationChange.WorkflowTerminated ended -> changes.add(new WorkflowEnded(ended.workflowId()));
            case WaitAssociationChange.WaitStarted opened -> changes.add(new WaitOpened(opened.association()));
            case WaitAssociationChange.WaitTerminated closed -> {
                // Every step that ends is reported as a closed wait. Only a wait has a row to remove.
                if (MetadataUtils.isWaitForEventStep(metadata)) {
                    changes.add(new WaitClosed(closed.workflowId(), closed.stepName()));
                }
            }
            case null -> {
                // Not an event that changes a row.
            }
        }
        if (!changes.isEmpty()) {
            pendingOf(segment).note(token, changes);
        }
    }

    /**
     * Notes that the given workflow ended, for a workflow a claim restored and found ended, so a row that outlived its
     * workflow is removed.
     *
     * @param segment    the segment that owns the workflow, or {@code null} outside a segmented processor
     * @param workflowId the identifier of the workflow
     */
    void ended(@Nullable Segment segment, String workflowId) {
        if (segment != null) {
            pendingOf(segment).note(null, List.of(new WorkflowEnded(workflowId)));
        }
    }

    /**
     * Notes that the given workflow hibernated, so its row gets the time it wakes at with the next stored token.
     * <p>
     * Ignored for an index that does not {@link #keepsWakeTimes() keep the wake times}.
     *
     * @param segment    the segment that owns the workflow, or {@code null} outside a segmented processor
     * @param workflowId the identifier of the workflow
     * @param wakesAt    the nearest timeout of its open waits, which wakes it
     */
    void wakesAt(@Nullable Segment segment, String workflowId, Instant wakesAt) {
        noteWakesAt(segment, workflowId, Optional.of(wakesAt));
    }

    /**
     * Notes that the given hibernated workflow was loaded back into memory, so its row loses the time it wakes at with
     * the next stored token.
     * <p>
     * Only a wake notes this. A workflow that leaves memory with its segment or with the engine is still hibernated in
     * the store, and the next claim must find it so.
     *
     * @param segment    the segment that owns the workflow, or {@code null} outside a segmented processor
     * @param workflowId the identifier of the workflow
     */
    void clearWakesAt(@Nullable Segment segment, String workflowId) {
        noteWakesAt(segment, workflowId, Optional.empty());
    }

    /**
     * Notes that the claim of the given segment restored the given workflow into memory. When its row carries the time
     * it wakes at, the row loses it: the workflow is not hibernated anymore, and a later claim must not take it so.
     * <p>
     * A claim that read the rows knows which rows carry a time. A claim that sourced does not, so the row of every
     * workflow it restores loses its time. A row without a time is left as it is.
     *
     * @param segment    the claimed segment
     * @param workflowId the identifier of the restored workflow
     */
    void restored(Segment segment, String workflowId) {
        var pendingChanges = pending.get(segment.getSegmentId());
        if (pendingChanges != null && pendingChanges.restoredWithWakeTime(workflowId)) {
            noteWakesAt(segment, workflowId, Optional.empty());
        }
    }

    private void noteWakesAt(@Nullable Segment segment, String workflowId, Optional<Instant> wakesAt) {
        if (segment != null && keepsWakeTimes()) {
            pendingOf(segment).noteWakesAt(workflowId, wakesAt);
        }
    }

    /**
     * Returns whether the rows keep the time each hibernated workflow wakes at, so a claim finds its hibernated
     * workflows in them.
     * <p>
     * Only an index that {@link WorkflowIndex#storesWithToken() stores with the token} does: a wake is a fact of one
     * node, so a node that lost its segment must not be able to store it. With any other index, the engine records its
     * hibernated workflows the way it does without an index.
     *
     * @return {@code true} when a claim reads its hibernated workflows from the rows
     */
    boolean keepsWakeTimes() {
        return index.storesWithToken();
    }

    /**
     * Stores the noted changes of the given segment that the given token covers, right before the processor stores
     * that token.
     * <p>
     * With an index that {@link WorkflowIndex#storesWithToken() stores with the token}, the changes join the
     * transaction of the given context and are forgotten once it commits. Otherwise they are stored in a transaction
     * of their own, before the token, so the rows are never behind the token.
     *
     * @param segment the segment the token is stored for
     * @param token   the position about to be stored
     * @param context the context of the transaction that stores the token
     * @return a future completing once the changes are stored
     */
    CompletableFuture<Void> store(Segment segment, TrackingToken token, ProcessingContext context) {
        var pendingChanges = pending.get(segment.getSegmentId());
        if (pendingChanges == null) {
            return CompletableFuture.completedFuture(null);
        }
        var covered = pendingChanges.coveredBy(token);
        var wakesAt = pendingChanges.wakesAt();
        var changes = collapsed(covered);
        wakesAt.forEach((workflowId, wakes) -> changes.add(
                wakes.<WorkflowIndexChange>map(time -> new WakesAtSet(workflowId, time))
                     .orElseGet(() -> new WakesAtCleared(workflowId))
        ));
        if (changes.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        logger.debug("Storing {} index change(s) of segment {} with token [{}].",
                     changes.size(), segment.getSegmentId(), token);
        if (!index.storesWithToken()) {
            return index.store(module, new WorkflowIndexChanges(changes, false), null)
                        .thenRun(() -> pendingChanges.forget(covered, wakesAt));
        }
        // Registered before the store, on the thread of the transaction. A rolled back transaction forgets nothing.
        context.runOnAfterCommit(committed -> pendingChanges.forget(covered, wakesAt));
        return index.store(module, new WorkflowIndexChanges(changes, false), context);
    }

    /**
     * Returns the given noted changes in their order, without the ones a later change in the same batch undoes: a
     * workflow that started and ended, and a wait that opened and closed. The change that ends or closes is kept, as it
     * may also remove a row stored earlier.
     */
    private static List<WorkflowIndexChange> collapsed(List<PendingChange> covered) {
        var ended = new HashSet<String>();
        var closed = new HashSet<WaitKey>();
        var kept = new ArrayList<WorkflowIndexChange>(covered.size());
        for (int i = covered.size() - 1; i >= 0; i--) {
            var change = covered.get(i).change();
            boolean keep = switch (change) {
                case WorkflowEnded workflowEnded -> ended.add(workflowEnded.workflowId());
                case WorkflowStarted started -> !ended.contains(started.workflowId());
                case WaitClosed waitClosed -> !ended.contains(waitClosed.workflowId())
                        && closed.add(new WaitKey(waitClosed.workflowId(), waitClosed.stepName()));
                case WaitOpened opened -> !ended.contains(opened.workflowId())
                        && !closed.contains(new WaitKey(opened.workflowId(), opened.association().stepName()));
                case WakesAtSet ignored -> true;
                case WakesAtCleared ignored -> true;
            };
            if (keep) {
                kept.add(change);
            }
        }
        Collections.reverse(kept);
        return kept;
    }

    /**
     * Answers the claim of the given segment from the index: the rows of the segment, brought up to the claim by the
     * lifecycle and wait events after the token the segment resumes from.
     * <p>
     * A workflow counts as hibernated when its row carries the time it wakes at and no event after the token belongs to
     * it. A workflow that made progress after it hibernated closed a wait or ended, and both are events the tail holds.
     *
     * @param segment         the claimed segment
     * @param claimedFrom     the stored position the segment resumes from, or {@code null} when it has consumed nothing
     * @param sourcingContext the context of the claim, whose resources the tail is read with
     * @return a future with the rows of the claim, or empty when the rows cannot answer it: the rows of the module are
     * not complete, the token gives no position to read the tail from, no event store is at hand, or reading the rows
     * or the tail failed. The claim then sources the {@link RunningWorkflows} and the {@link WaitAssociations}
     */
    CompletableFuture<Optional<ClaimedRows>> claim(Segment segment,
                                                   @Nullable TrackingToken claimedFrom,
                                                   ProcessingContext sourcingContext) {
        // Changes noted while this node held the segment before are covered by the rows and the tail read now.
        pending.remove(segment.getSegmentId());
        var tailStart = claimedFrom == null ? OptionalLong.empty() : tailStart(claimedFrom);
        if (tailStart.isEmpty()) {
            return CompletableFuture.completedFuture(sourced(segment));
        }
        EventStore eventStore;
        try {
            eventStore = sourcingContext.component(EventStore.class);
        } catch (ComponentNotFoundException e) {
            return CompletableFuture.completedFuture(sourced(segment));
        }
        return index.isComplete(module, null).thenCompose(complete -> {
            if (!complete) {
                return CompletableFuture.completedFuture(sourced(segment));
            }
            return index.load(module, segment, null)
                        .thenCompose(rows -> readTail(
                                new Claim(segment, rows), tailStart.getAsLong(), eventStore, sourcingContext
                        ))
                        .thenApply(claim -> {
                            var claimed = claim.rows(keepsWakeTimes());
                            pendingOf(segment).hinted(claim.hinted());
                            logger.debug("Claimed segment {} from the index: {} running workflow(s), {} open "
                                                 + "wait(s), {} hibernated.",
                                         segment.getSegmentId(), claimed.runningWorkflowIds().size(),
                                         claimed.waits().size(), claimed.hibernated().workflows().size());
                            return Optional.of(claimed);
                        });
        }).exceptionally(failure -> {
            // A claim that fails restores nothing, while its segment still runs. Sourcing restores every workflow.
            logger.warn("Could not claim segment {} of module [{}] from the workflow index; sourcing the claim "
                                + "instead.", segment.getSegmentId(), module, failure);
            return sourced(segment);
        });
    }

    /**
     * Returns that the rows cannot answer the claim of the given segment, so the claim sources instead. The rows may
     * still carry wake times, so every workflow the claim restores loses its time, see
     * {@link #restored(Segment, String)}.
     */
    private Optional<ClaimedRows> sourced(Segment segment) {
        pendingOf(segment).sourced();
        return Optional.empty();
    }

    /**
     * Returns the position the tail of a claim starts at: the lowest position the given token may still have to
     * consume. Starting early is safe, as applying an event again changes nothing. Starting late would miss an event.
     */
    private static OptionalLong tailStart(TrackingToken claimedFrom) {
        var lowerBound = WrappedToken.unwrapLowerBound(claimedFrom);
        if (lowerBound instanceof GapAwareTrackingToken gapAware && gapAware.hasGaps()) {
            return OptionalLong.of(gapAware.getGaps().first());
        }
        return lowerBound == null ? claimedFrom.position() : lowerBound.position();
    }

    /**
     * Applies the lifecycle and wait events from the given position on to the given claim.
     * <p>
     * Read in a unit of work of its own, as each workflow is restored in one: the position a read ends at belongs to
     * its transaction, and must not become the position of another read.
     */
    private CompletableFuture<Claim> readTail(Claim claim,
                                              long position,
                                              EventStore eventStore,
                                              ProcessingContext sourcingContext) {
        var condition = SourcingCondition.conditionFor(new GlobalIndexPosition(position),
                                                       EventSourcedWaitAssociations.criteriaBuilder());
        return unitOfWorkFactory
                .create("index-tail-" + module + "-" + claim.segment.getSegmentId())
                .executeWithResult(tailContext -> {
                    ProcessingContextUtils.copyResources(sourcingContext, tailContext);
                    return eventStore.transaction(tailContext)
                                     .source(condition)
                                     .reduce(claim, (current, entry) -> claim.apply(entry.message()));
                });
    }

    /**
     * Stores the given running workflows and open waits as the rows of this module, and marks the module complete, so
     * claims read the rows from then on. Does nothing when the module is complete.
     * <p>
     * Called by a claim that sourced the {@link RunningWorkflows} and the {@link WaitAssociations} because the rows
     * were not complete. Both hold every segment, so one claim completes the module. Several nodes may do so at once,
     * as storing a row again has no further effect.
     *
     * @param runningWorkflowIds the identifiers of every running workflow, of every segment
     * @param waits              every open wait, of every segment
     * @return a future completing once the rows are stored
     */
    CompletableFuture<Void> seed(Collection<String> runningWorkflowIds, Collection<WaitAssociation> waits) {
        return index.isComplete(module, null).thenCompose(complete -> {
            if (complete) {
                return CompletableFuture.completedFuture(null);
            }
            var changes = new ArrayList<WorkflowIndexChange>(runningWorkflowIds.size() + waits.size());
            runningWorkflowIds.forEach(workflowId -> changes.add(new WorkflowStarted(workflowId)));
            waits.forEach(wait -> changes.add(new WaitOpened(wait)));
            logger.info("Completing the workflow index of module [{}] with {} running workflow(s) and {} open "
                                + "wait(s).", module, runningWorkflowIds.size(), waits.size());
            return index.store(module, new WorkflowIndexChanges(changes, true), null);
        });
    }

    /**
     * Forgets every noted change, when the engine stops.
     */
    void clear() {
        pending.clear();
    }

    private PendingChanges pendingOf(Segment segment) {
        return pending.computeIfAbsent(segment.getSegmentId(), segmentId -> new PendingChanges());
    }

    /**
     * What a claim of a segment may use instead of sourcing the {@link RunningWorkflows}, the {@link WaitAssociations}
     * and the recorded {@link HibernatedWorkflows}.
     *
     * @param runningWorkflowIds the identifiers of the running workflows of the segment
     * @param waits              the open waits of the segment
     * @param hibernated         the workflows of the segment to keep out of memory
     */
    record ClaimedRows(Set<String> runningWorkflowIds,
                       List<WaitAssociation> waits,
                       HibernatedWorkflows hibernated) {

    }

    /**
     * The noted changes of one segment. Guarded by its lock: the worker of the segment notes and stores, while
     * hibernation and a claim note from their own threads.
     */
    private static final class PendingChanges {

        private final ReentrantLock lock = new ReentrantLock();
        private final List<PendingChange> changes = new ArrayList<>();
        // The latest state per workflow: the time it wakes at, or empty when it woke.
        private final Map<String, Optional<Instant>> wakesAt = new HashMap<>();
        // The workflows whose row carried a wake time when the segment was claimed.
        private final Set<String> hinted = new HashSet<>();
        // Whether the claim sourced instead of reading the rows, so it does not know which rows carry a wake time.
        private boolean sourced;

        private void note(@Nullable TrackingToken token, List<WorkflowIndexChange> noted) {
            lock.lock();
            try {
                noted.forEach(change -> changes.add(new PendingChange(token, change)));
            } finally {
                lock.unlock();
            }
        }

        private void noteWakesAt(String workflowId, Optional<Instant> wakes) {
            lock.lock();
            try {
                // Only the latest state of a workflow is stored.
                wakesAt.put(workflowId, wakes);
            } finally {
                lock.unlock();
            }
        }

        private void hinted(Set<String> workflowIds) {
            lock.lock();
            try {
                hinted.addAll(workflowIds);
            } finally {
                lock.unlock();
            }
        }

        private void sourced() {
            lock.lock();
            try {
                sourced = true;
            } finally {
                lock.unlock();
            }
        }

        /**
         * Returns whether the row of the given restored workflow may carry a wake time, and forgets that it did.
         */
        private boolean restoredWithWakeTime(String workflowId) {
            lock.lock();
            try {
                return hinted.remove(workflowId) || sourced;
            } finally {
                lock.unlock();
            }
        }

        private List<PendingChange> coveredBy(TrackingToken token) {
            lock.lock();
            try {
                return changes.stream().filter(change -> change.coveredBy(token)).toList();
            } finally {
                lock.unlock();
            }
        }

        private Map<String, Optional<Instant>> wakesAt() {
            lock.lock();
            try {
                return new HashMap<>(wakesAt);
            } finally {
                lock.unlock();
            }
        }

        private void forget(List<PendingChange> stored, Map<String, Optional<Instant>> storedWakesAt) {
            Set<PendingChange> storedChanges = Collections.newSetFromMap(new IdentityHashMap<>());
            storedChanges.addAll(stored);
            lock.lock();
            try {
                changes.removeIf(storedChanges::contains);
                // A state noted since the store stays, as it is not stored yet.
                storedWakesAt.forEach(wakesAt::remove);
            } finally {
                lock.unlock();
            }
        }
    }

    private record PendingChange(@Nullable TrackingToken token, WorkflowIndexChange change) {

        private boolean coveredBy(TrackingToken stored) {
            return token == null || TrackingTokenUtils.coversWhenUnwrapped(stored, token);
        }
    }

    private record WaitKey(String workflowId, String stepName) {

    }

    /**
     * The rows of a claimed segment while the tail is applied to them. Used by one thread at a time.
     */
    private static final class Claim {

        private final Segment segment;
        private final Set<String> running;
        private final Map<String, Instant> wakesAt;
        private final Map<WaitKey, WaitAssociation> waits = new LinkedHashMap<>();
        private final Set<String> touched = new HashSet<>();

        private Claim(Segment segment, WorkflowIndexRows rows) {
            this.segment = segment;
            this.running = new HashSet<>(rows.runningWorkflowIds());
            this.wakesAt = rows.wakesAt();
            // Keyed, so a wait stored twice counts once.
            rows.waits().forEach(wait -> waits.put(new WaitKey(wait.workflowId(), wait.stepName()), wait));
        }

        private Claim apply(EventMessage event) {
            var metadata = event.metadata();
            if (!MetadataUtils.hasWorkflowId().test(metadata)) {
                return this;
            }
            var workflowId = MetadataUtils.getWorkflowId(metadata);
            if (!WorkflowSegmentOwnership.ownedBy(segment, workflowId)) {
                return this;
            }
            touched.add(workflowId);
            if (MetadataUtils.getWorkflowStatus(metadata)
                             .filter(status -> status == WorkflowStatus.STARTED)
                             .isPresent()) {
                running.add(workflowId);
            }
            switch (WaitAssociationChange.of(event)) {
                case WaitAssociationChange.WorkflowTerminated ended -> {
                    running.remove(workflowId);
                    waits.keySet().removeIf(key -> key.workflowId().equals(workflowId));
                }
                case WaitAssociationChange.WaitStarted opened -> waits.put(
                        new WaitKey(workflowId, opened.association().stepName()), opened.association()
                );
                case WaitAssociationChange.WaitTerminated closed -> waits.remove(
                        new WaitKey(workflowId, closed.stepName())
                );
                case null -> {
                    // Not an event that changes a row.
                }
            }
            return this;
        }

        /**
         * Returns the running workflows whose row carries a wake time, adopted as hibernated or not.
         */
        private Set<String> hinted() {
            var hinted = new HashSet<>(wakesAt.keySet());
            hinted.retainAll(running);
            return hinted;
        }

        private ClaimedRows rows(boolean adoptHibernated) {
            var hibernated = new HashMap<String, HibernatedWorkflows.HibernatedWorkflow>();
            if (adoptHibernated && !wakesAt.isEmpty()) {
                var openWaits = new HashMap<String, Set<String>>();
                waits.keySet().forEach(key -> openWaits.computeIfAbsent(key.workflowId(), id -> new HashSet<>())
                                                       .add(key.stepName()));
                wakesAt.forEach((workflowId, wakes) -> {
                    if (running.contains(workflowId) && !touched.contains(workflowId)) {
                        hibernated.put(workflowId, new HibernatedWorkflows.HibernatedWorkflow(
                                openWaits.getOrDefault(workflowId, Set.of()), wakes.toString()
                        ));
                    }
                });
            }
            return new ClaimedRows(Set.copyOf(running),
                                   List.copyOf(waits.values()),
                                   new HibernatedWorkflows(hibernated));
        }
    }
}
