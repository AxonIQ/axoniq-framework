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
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges.SegmentPosition;
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

import java.time.Duration;
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
import java.util.function.Predicate;

/**
 * Keeps the {@link WorkflowIndex} of a {@link WorkflowEngine} current, and answers a segment claim and a query of the
 * workflow manager for live workflows from it.
 * <p>
 * Row changes are noted while the engine handles events, and stored with the token that covers them. A claim reads
 * the rows of its segment, then the tail: the lifecycle and wait events from the token the segment resumes from. Rows
 * plus tail equal sourcing the {@link RunningWorkflows} and {@link WaitAssociations}. Storing a change twice has no
 * effect. Until the module is initialized, a claim sources both lists and initializes the index with them.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
final class WorkflowEngineIndexSupport {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngineIndexSupport.class);

    /**
     * How long the position of a segment may lag behind its token while the segment changes no row.
     */
    static final Duration POSITION_INTERVAL = Duration.ofSeconds(10);

    private final WorkflowIndex index;
    private final String module;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Predicate<String> runsHere;
    private final Map<Integer, PendingChanges> pending = new ConcurrentHashMap<>();

    /**
     * Creates the index support of an engine.
     *
     * @param index             the index to keep current and to claim from
     * @param module            the name of the workflow module, which keeps its rows apart from those of other modules
     * @param unitOfWorkFactory creates the unit of work the tail of a claim is read in
     * @param runsHere          whether the engine holds a workflow that may still hibernate: in memory and not
     *                          terminal, or hibernated
     */
    WorkflowEngineIndexSupport(WorkflowIndex index,
                               String module,
                               UnitOfWorkFactory unitOfWorkFactory,
                               Predicate<String> runsHere) {
        this.index = Objects.requireNonNull(index, "The WorkflowIndex must not be null.");
        this.module = Objects.requireNonNull(module, "The module name must not be null.");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "The UnitOfWorkFactory must not be null.");
        this.runsHere = Objects.requireNonNull(runsHere, "The runs here predicate must not be null.");
    }

    /**
     * Notes the row changes of the given event, also for a workflow whose restore failed, so a later claim finds it.
     *
     * @param segment the segment that handles the event, or {@code null} outside a segmented processor
     * @param token   the position of the event, or {@code null} when it is not known
     * @param event   an event that carries a workflow id and that the segment owns
     */
    void record(@Nullable Segment segment, @Nullable TrackingToken token, EventMessage event) {
        if (segment == null) {
            return;
        }
        var changes = changesOf(event);
        if (!changes.isEmpty()) {
            pendingOf(segment).note(token, changes);
        }
    }

    private static List<WorkflowIndexChange> changesOf(EventMessage event) {
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
                // Every ended step is reported as a closed wait, but only a wait has a row.
                if (MetadataUtils.isWaitForEventStep(metadata)) {
                    changes.add(new WaitClosed(closed.workflowId(), closed.stepName()));
                }
            }
            case null -> {
                // Not an event that changes a row.
            }
        }
        return changes;
    }

    /**
     * Notes that a workflow a claim restored had already ended, so its outdated row is removed.
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
     * Notes that the given workflow hibernated, unless the index does not {@link #keepsWakeTimes() keep wake times} or
     * the end of the workflow was noted. A copy that missed its end, as another node ended it, hibernates as if it
     * still ran, and its wake time would add the removed row again.
     *
     * @param segment    the segment that owns the workflow, or {@code null} outside a segmented processor
     * @param workflowId the identifier of the workflow
     * @param wakesAt    the nearest timeout of its open waits, which wakes it
     */
    void wakesAt(@Nullable Segment segment, String workflowId, Instant wakesAt) {
        noteWakesAt(segment, workflowId, Optional.of(wakesAt));
    }

    /**
     * Notes that the given workflow woke. Only a wake clears the wake time: a dropped workflow is still hibernated.
     *
     * @param segment    the segment that owns the workflow, or {@code null} outside a segmented processor
     * @param workflowId the identifier of the workflow
     */
    void clearWakesAt(@Nullable Segment segment, String workflowId) {
        noteWakesAt(segment, workflowId, Optional.empty());
    }

    /**
     * Notes that a claim restored the given workflow, so its row loses any wake time it may carry.
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
     * Returns whether the rows keep wake times: only with an index that
     * {@link WorkflowIndex#storesWithToken() stores with the token}, so a node that lost its segment cannot store one.
     *
     * @return {@code true} when a claim reads its hibernated workflows from the rows
     */
    boolean keepsWakeTimes() {
        return index.storesWithToken();
    }

    /**
     * Stores the noted changes the given token covers, right before the processor stores that token. The position of
     * the segment, which only the workflow manager reads, moves to the token with every change, and at least every
     * {@link #POSITION_INTERVAL}.
     *
     * @param segment the segment the token is stored for
     * @param token   the token about to be stored
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
        var tokenPosition = tailStart(token);
        boolean movePosition = tokenPosition.isPresent() && (!changes.isEmpty() || pendingChanges.positionDue());
        if (changes.isEmpty() && !movePosition) {
            return CompletableFuture.completedFuture(null);
        }
        var position = movePosition
                ? new SegmentPosition(SegmentKeyRange.of(segment), tokenPosition.getAsLong())
                : null;
        var stored = new WorkflowIndexChanges(changes, position);
        logger.debug("Storing {} index change(s) of segment {} with token [{}].",
                     changes.size(), segment.getSegmentId(), token);
        if (!index.storesWithToken()) {
            return index.store(module, stored, null)
                        .thenRun(() -> pendingChanges.forget(covered, wakesAt, movePosition));
        }
        // Registered before the store, on the thread of the transaction. A rolled back transaction forgets nothing.
        context.runOnAfterCommit(committed -> pendingChanges.forget(covered, wakesAt, movePosition));
        return index.store(module, stored, context);
    }

    /**
     * Drops the changes a later change in the batch undoes. The end or close stays, as it may remove an older row.
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
     * Answers the claim of the given segment from its rows and the tail. A workflow counts as hibernated when its row
     * carries a wake time and the tail holds none of its events: a workflow that progressed closed a wait or ended.
     *
     * @param segment         the claimed segment
     * @param claimedFrom     the stored token the segment resumes from, or {@code null} when it has consumed nothing
     * @param sourcingContext the context of the claim, whose resources the tail is read with
     * @return a future with the rows of the claim, or empty when the rows cannot answer it and the claim must source,
     * see {@link #sourced(Segment, Collection, Collection)}
     */
    CompletableFuture<Optional<ClaimedRows>> claim(Segment segment,
                                                   @Nullable TrackingToken claimedFrom,
                                                   ProcessingContext sourcingContext) {
        // Changes noted while this node held the segment before are covered by the rows and the tail read now.
        pending.remove(segment.getSegmentId());
        return index.load(module, segment, null).thenCompose(loaded -> {
            if (loaded.isEmpty()) {
                pendingOf(segment).initializing();
                return CompletableFuture.completedFuture(sourced(segment));
            }
            var rows = loaded.get();
            var tailStart = claimedFrom == null ? OptionalLong.empty() : tailStart(claimedFrom);
            var eventStore = eventStoreOf(sourcingContext);
            if (eventStore == null || tailStart.isEmpty()) {
                return CompletableFuture.completedFuture(sourced(segment, rows.runningWorkflowIds()));
            }
            long from = tailStart.getAsLong();
            return readTail(new Claim(segment, rows), from, eventStore, sourcingContext).thenApply(claim -> {
                var claimed = claim.rows(keepsWakeTimes());
                var pendingChanges = pendingOf(segment);
                pendingChanges.hinted(claim.hinted());
                pendingChanges.note(null, claim.changes());
                logger.debug("Claimed segment {} from the index: {} running workflow(s), {} open wait(s), {} "
                                     + "hibernated, {} change(s) read from position {}.",
                             segment.getSegmentId(), claimed.runningWorkflowIds().size(), claimed.waits().size(),
                             claimed.hibernated().workflows().size(), claim.changes().size(), from);
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
     * Returns which of the given workflows run, the way a claim of every segment at once finds them: from their rows,
     * and the lifecycle events after the lowest token position of the module.
     *
     * @param workflowIds the identifiers of the workflows to check
     * @param context     the context whose resources the events are read with
     * @return a future with the running workflows, or empty when the module is not initialized, a segment has no
     * position, or the rows cannot be read, so the caller sources the {@link RunningWorkflows} instead
     */
    CompletableFuture<Optional<Set<String>>> running(Collection<String> workflowIds, ProcessingContext context) {
        var eventStore = eventStoreOf(context);
        if (eventStore == null) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return index.loadWorkflows(module, workflowIds, null).thenCompose(loaded -> {
            var position = loaded.map(rows -> rows.positionOf(Segment.ROOT_SEGMENT)).orElse(OptionalLong.empty());
            if (position.isEmpty()) {
                return CompletableFuture.completedFuture(Optional.<Set<String>>empty());
            }
            return readTail(new Claim(Segment.ROOT_SEGMENT, loaded.get()), position.getAsLong(), eventStore, context)
                    .thenApply(claim -> Optional.of(claim.rows(false).runningWorkflowIds()));
        }).exceptionally(failure -> {
            logger.warn("Could not read the running workflows of module [{}] from the workflow index; sourcing them "
                                + "instead.", module, failure);
            return Optional.empty();
        });
    }

    private static @Nullable EventStore eventStoreOf(ProcessingContext context) {
        try {
            return context.component(EventStore.class);
        } catch (ComponentNotFoundException e) {
            return null;
        }
    }

    private Optional<ClaimedRows> sourced(Segment segment) {
        return sourced(segment, Set.of());
    }

    private Optional<ClaimedRows> sourced(Segment segment, Set<String> rowsOfSegment) {
        pendingOf(segment).sourced(rowsOfSegment);
        return Optional.empty();
    }

    /**
     * Returns the lowest position the given token may still have to consume. Starting early is safe, late is not.
     */
    private static OptionalLong tailStart(TrackingToken claimedFrom) {
        var lowerBound = WrappedToken.unwrapLowerBound(claimedFrom);
        if (lowerBound instanceof GapAwareTrackingToken gapAware && gapAware.hasGaps()) {
            return OptionalLong.of(gapAware.getGaps().first());
        }
        return lowerBound == null ? claimedFrom.position() : lowerBound.position();
    }

    /**
     * Reads the tail in a unit of work of its own, so the position the read ends at never leaks into another read.
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
     * Notes the running workflows and open waits a claim sourced as the rows of the segment, replacing all others.
     * When the claim found the module not initialized, also initializes the index with them.
     *
     * @param segment            the claimed segment
     * @param runningWorkflowIds the identifiers of the sourced running workflows, of any segment
     * @param waits              the sourced open waits, of any segment
     * @return a future completing once the index is initialized, or at once when it needs no initialization
     */
    CompletableFuture<Void> sourced(Segment segment,
                                    Collection<String> runningWorkflowIds,
                                    Collection<WaitAssociation> waits) {
        var pendingChanges = pendingOf(segment);
        var changes = new ArrayList<WorkflowIndexChange>();
        var running = new HashSet<String>();
        runningWorkflowIds.stream()
                          .filter(workflowId -> WorkflowSegmentOwnership.ownedBy(segment, workflowId))
                          .forEach(workflowId -> {
                              running.add(workflowId);
                              // Replaced, so a wait the workflow closed while no row was kept does not stay.
                              changes.add(new WorkflowEnded(workflowId));
                              changes.add(new WorkflowStarted(workflowId));
                          });
        pendingChanges.rowsAtClaim()
                      .stream()
                      .filter(workflowId -> !running.contains(workflowId))
                      .forEach(workflowId -> changes.add(new WorkflowEnded(workflowId)));
        waits.stream()
             .filter(wait -> running.contains(wait.workflowId()))
             .forEach(wait -> changes.add(new WaitOpened(wait)));
        logger.debug("Sourced segment {} of module [{}]: {} running workflow(s) become its rows.",
                     segment.getSegmentId(), module, running.size());
        pendingChanges.note(null, changes);
        if (!pendingChanges.initializes()) {
            return CompletableFuture.completedFuture(null);
        }
        return index.initialize(module, Set.copyOf(runningWorkflowIds), waits).handle((initialized, failure) -> {
            if (failure != null) {
                // The rows of this segment are still stored with the next token. A later claim initializes again.
                logger.warn("Could not initialize the workflow index for module [{}].", module, failure);
            } else {
                logger.debug("Initialized the workflow index for module [{}] with {} running workflow(s).",
                             module, runningWorkflowIds.size());
            }
            return null;
        });
    }

    void clear() {
        pending.clear();
    }

    private PendingChanges pendingOf(Segment segment) {
        return pending.computeIfAbsent(segment.getSegmentId(), segmentId -> new PendingChanges(runsHere));
    }

    /**
     * What a claim of a segment uses instead of sourcing.
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
     * The noted changes of one segment, behind a lock, as hibernation and a claim note from their own threads.
     */
    private static final class PendingChanges {

        private final ReentrantLock lock = new ReentrantLock();
        private final Predicate<String> runsHere;
        private final List<PendingChange> changes = new ArrayList<>();
        // The wake time per workflow, or empty when it woke.
        private final Map<String, Optional<Instant>> wakesAt = new HashMap<>();
        // The workflows whose row carried a wake time when the segment was claimed.
        private final Set<String> hinted = new HashSet<>();
        // The ended workflows the engine may still hold a copy of, which takes no wake time.
        private final Set<String> ended = new HashSet<>();
        private boolean sourced;
        private boolean initializes;
        private Set<String> rowsAtClaim = Set.of();
        private boolean positionStored;
        private long positionStoredAt;

        private PendingChanges(Predicate<String> runsHere) {
            this.runsHere = runsHere;
        }

        private void note(@Nullable TrackingToken token, List<WorkflowIndexChange> noted) {
            lock.lock();
            try {
                noted.forEach(change -> {
                    changes.add(new PendingChange(token, change));
                    if (change instanceof WorkflowEnded workflowEnded) {
                        ended.add(workflowEnded.workflowId());
                        wakesAt.remove(workflowEnded.workflowId());
                    } else if (change instanceof WorkflowStarted started) {
                        // A sourced claim replaces a row by an end and a start.
                        ended.remove(started.workflowId());
                    }
                });
            } finally {
                lock.unlock();
            }
        }

        private void noteWakesAt(String workflowId, Optional<Instant> wakes) {
            lock.lock();
            try {
                if (!ended.contains(workflowId)) {
                    wakesAt.put(workflowId, wakes);
                }
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

        private void sourced(Set<String> rowsOfSegment) {
            lock.lock();
            try {
                sourced = true;
                rowsAtClaim = Set.copyOf(rowsOfSegment);
            } finally {
                lock.unlock();
            }
        }

        private void initializing() {
            lock.lock();
            try {
                initializes = true;
            } finally {
                lock.unlock();
            }
        }

        private boolean initializes() {
            lock.lock();
            try {
                return initializes;
            } finally {
                lock.unlock();
            }
        }

        private Set<String> rowsAtClaim() {
            lock.lock();
            try {
                return rowsAtClaim;
            } finally {
                lock.unlock();
            }
        }

        private boolean positionDue() {
            lock.lock();
            try {
                return !positionStored || System.nanoTime() - positionStoredAt > POSITION_INTERVAL.toNanos();
            } finally {
                lock.unlock();
            }
        }

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

        private void forget(List<PendingChange> stored,
                            Map<String, Optional<Instant>> storedWakesAt,
                            boolean positionStored) {
            Set<PendingChange> storedChanges = Collections.newSetFromMap(new IdentityHashMap<>());
            storedChanges.addAll(stored);
            lock.lock();
            try {
                changes.removeIf(storedChanges::contains);
                // Removes only the stored value, so a wake time noted since the store stays.
                storedWakesAt.forEach(wakesAt::remove);
                // A workflow the engine no longer runs notes no wake time.
                ended.removeIf(runsHere.negate());
                if (positionStored) {
                    this.positionStored = true;
                    positionStoredAt = System.nanoTime();
                }
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
     * The rows of a claimed segment while the tail is applied. Used by one thread at a time.
     */
    private static final class Claim {

        private final Segment segment;
        private final Set<String> running;
        private final Map<String, Instant> wakesAt;
        private final Map<WaitKey, WaitAssociation> waits = new LinkedHashMap<>();
        private final Set<String> touched = new HashSet<>();
        private final List<WorkflowIndexChange> changes = new ArrayList<>();

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
            changes.addAll(changesOf(event));
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

        private List<WorkflowIndexChange> changes() {
            return changes;
        }

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
                                openWaits.getOrDefault(workflowId, Set.of()), wakes
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
