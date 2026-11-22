/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.IndexDelta;
import io.axoniq.framework.workflow.runtime.execution.RunningWorkflowsClaimLoader.ClaimedIndex;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.runtime.util.WorkflowIds;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Keeps, per claimed segment, which workflow instances are running, and makes changes to that set durable through a
 * {@link WorkflowIndex} whenever the processor takes a checkpoint of the segment.
 * <p>
 * The {@link WorkflowEngine} owns one projection and drives it from these places:
 * <ul>
 *     <li>On claim, once a {@link RunningWorkflowsClaimLoader} loaded the instances to restore and the changes the
 *     index does not hold yet, {@link #initialize(Segment, ClaimedIndex)} starts tracking the segment.</li>
 *     <li>On the handler thread, {@link #apply(Segment, EventMessage)} applies every handled lifecycle event of an
 *     owned instance, keeping the change in memory.</li>
 *     <li>With hibernation, {@link #recordHibernated(Segment, String, HibernationHint)} and
 *     {@link #recordWoken(Segment, String)} record the {@link HibernationHint hint} of an instance that left memory,
 *     and clear it again when it wakes.</li>
 *     <li>Every checkpoint of the segment decides to write every pending change through
 *     {@link #decideCheckpoint(Segment)}, writes them once the workflows of the segment are safe, and fails when that
 *     write fails or does not complete in time.</li>
 *     <li>On release, {@link #flushForRelease(Segment)} writes the pending changes within the release timeout, in a
 *     transaction of the index's own, and {@link #discard(Segment)} stops tracking the segment.</li>
 * </ul>
 * A checkpoint write gets the context in which the processor stores the token, so an index in the same database can
 * write within that transaction, see {@link WorkflowIndex#storeWithin(String, IndexDelta, ProcessingContext)}. Such a write
 * is durable only once the context commits: when the context fails instead, its changes become pending again, unless
 * a later change of the same instance replaced them.
 *
 * <h2>Hibernation hints</h2>
 * A hint is not derived from the events, so a write that sets one must never land after a later claim of the segment
 * cleared it. A change that sets a hint is therefore only written by a checkpoint whose write joins the transaction in
 * which the processor stores its token, see {@link WorkflowIndex#storeWithin(String, IndexDelta, ProcessingContext)}.
 * Every other write, including that of a release, writes the instance without a hint instead, and keeps the hint
 * pending for a later checkpoint; the hints that are still pending when the segment is released are dropped. Clearing
 * a hint, adding and removing instances are written by every write. As a safety net, any event of an instance whose
 * latest recorded change set a hint clears that hint, as the instance then no longer waits where it hibernated.
 *
 * <h2>Why a stored token never passes an unwritten change</h2>
 * The processor stores a token of a segment that handled events only after a checkpoint, and every checkpoint writes
 * every change applied before it was taken first. So once a token is stored, every change that an event up to that
 * token caused is durable. A checkpoint whose write fails fails as well: the processor releases the segment and claims
 * it again, and the claim restores the running instances from the stored entries and the lifecycle events after the
 * stored token.
 * <p>
 * Without a checkpoint, the processor only advances the stored token past events it skipped, as long as it handed none
 * since the stored token. Skipped events carry no lifecycle event of an instance the segment owns, as those are routed
 * to it. The only changes that can be pending while nothing was handled are those a claim found: changes caused by
 * lifecycle events after the token, which are handed to the segment rather than skipped, additions the initialized
 * index already holds, as a claim that could not load the index adds every running instance, removals of instances
 * whose state is terminal, which are safe at any token, or changes of an index that is not initialized, which no claim
 * relies on.
 *
 * <h2>Why entries ahead of the token converge</h2>
 * A claim restores the stored entries plus the lifecycle events from the token the segment resumes from. Whether an
 * instance is running is decided by its last lifecycle event, so applying an ordered run of lifecycle events converges
 * to the same set whatever the entries already reflect. Entries ahead of the token, written by a checkpoint whose token
 * a crash prevented from being stored, by a checkpoint that failed for another reason, or by a write that landed after
 * the processor released the segment, are thus harmless. Writes are not ordered across claims: a write of an earlier
 * claim that lands after one of a later claim can only leave entries of instances that are no longer running, never
 * drop a running one, as an instance never runs again once it ended. A later claim removes such entries once it finds
 * their sourced state terminal, see {@link ClaimedIndex#withoutStale(Set)}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
final class RunningWorkflowsProjection {

    /**
     * Time a checkpoint waits for the pending changes of its segment to be written.
     * <p>
     * A write that does not complete in time fails the checkpoint with a failure of the index, so the processor
     * releases the segment and claims it again, rather than stalling the segment for as long as the index does not
     * respond. It is short enough to fail the checkpoint before a processor that bounds its own wait for a checkpoint
     * gives up on it.
     */
    static final Duration DEFAULT_CHECKPOINT_TIMEOUT = Duration.ofSeconds(20);
    /**
     * Time a release waits for its pending changes to be written.
     * <p>
     * A pooled streaming event processor first asks for the token to store on release, waiting at most thirty seconds
     * for it. The engine answers once the workflow work of the segment is safe and its pending changes are written,
     * waiting for the writes at most this long. Only then does the processor call its release listeners, which stop
     * the executions of the segment, bounded by its claim extension threshold, five seconds by default. As the claim of
     * the segment is no longer extended meanwhile, a slow index must not hold up the release long. A release that does
     * not get its changes written in time does not advance the stored token, which leaves the next claim more events
     * to read.
     */
    static final Duration DEFAULT_RELEASE_TIMEOUT = Duration.ofSeconds(3);
    private static final Logger logger = LoggerFactory.getLogger(RunningWorkflowsProjection.class);

    private final WorkflowIndex workflowIndex;
    // Whether the index keeps anything at all: WorkflowIndex.none() is bypassed entirely.
    private final boolean indexed;
    private final String indexName;
    private final Map<Integer, SegmentProjection> segments = new ConcurrentHashMap<>();
    // Package-private so a test can shrink it instead of waiting out the production timeout.
    Duration checkpointTimeout = DEFAULT_CHECKPOINT_TIMEOUT;
    // Package-private so a test can shrink it instead of waiting out the production timeout.
    Duration releaseTimeout = DEFAULT_RELEASE_TIMEOUT;

    /**
     * Creates a projection persisting through the given workflow index.
     *
     * @param workflowIndex the index persisting the running workflows
     * @param indexName     the name scoping this projection's entries in the {@code workflowIndex}
     */
    RunningWorkflowsProjection(WorkflowIndex workflowIndex, String indexName) {
        this.workflowIndex = requireNonNull(workflowIndex, "The WorkflowIndex must not be null.");
        this.indexed = workflowIndex != WorkflowIndex.none();
        this.indexName = requireNonNull(indexName, "The index name must not be null.");
    }

    /**
     * Returns whether the given event marks its workflow instance as running, not running, or does not change whether
     * it runs (empty).
     * <p>
     * Only workflow lifecycle events change it. Events published by a workflow through the publish primitive carry the
     * publisher's id, but are business events and never do.
     *
     * @param metadata the metadata of an event
     * @return {@code true} for a start, {@code false} for a terminal status, empty otherwise
     */
    static Optional<Boolean> membershipChange(Metadata metadata) {
        if (MetadataUtils.isPublishStep(metadata) || !MetadataUtils.hasWorkflowId().test(metadata)) {
            return Optional.empty();
        }
        return MetadataUtils.getWorkflowStatus(metadata).flatMap(status -> switch (status) {
            case STARTED -> Optional.of(true);
            case COMPLETED, FAILED, TIMED_OUT, CANCELLED -> Optional.of(false);
            case NONE -> Optional.empty();
        });
    }

    /**
     * Starts tracking the given {@code segment} from what its claim loaded, unless the index is
     * {@link WorkflowIndex#none()}, which keeps nothing to track.
     * <p>
     * The changes the claim found beyond the stored entries start out pending, and are written with the first
     * checkpoint or the release of the segment. The loaded set reflects the head of the stream, which is at or after
     * the token the segment resumes from, so it may be ahead of that token. That is safe: handling then re-applies
     * lifecycle events the set already reflects, which converges to the same set.
     *
     * @param segment the claimed segment
     * @param claimed what the claim loaded
     */
    void initialize(Segment segment, ClaimedIndex claimed) {
        if (indexed) {
            segments.put(segment.getSegmentId(),
                         new SegmentProjection(segment, claimed.changes(), claimed.hibernated().keySet()));
        }
    }

    /**
     * Records that the given workflow instance of the given {@code segment} hibernated with the given {@code hint}.
     * <p>
     * The hint is written by the next checkpoint write of the segment that joins the transaction of the processor, see
     * {@link WorkflowIndex#storeWithin(String, IndexDelta, ProcessingContext)}, also when that checkpoint was decided
     * before the hint was recorded, see {@link #decideCheckpoint(Segment)}. Ignored when the segment is not tracked.
     *
     * @param segment    the segment owning the instance
     * @param workflowId the id of the hibernated instance
     * @param hint       the open waits and nearest deadline the instance hibernated with
     */
    void recordHibernated(Segment segment, String workflowId, HibernationHint hint) {
        requireNonNull(hint, "The hibernation hint must not be null.");
        withProjectionOf(segment, projection -> projection.recordHint(workflowId, hint));
    }

    /**
     * Records that the given workflow instance of the given {@code segment} is in memory again, clearing the hint it
     * hibernated with. Does nothing when no hint of the instance is recorded or stored, or when the segment is not
     * tracked.
     *
     * @param segment    the segment owning the instance
     * @param workflowId the id of the instance that woke
     */
    void recordWoken(Segment segment, String workflowId) {
        withProjectionOf(segment, projection -> projection.clearHint(workflowId));
    }

    /**
     * Records that the given workflow instance of the given {@code segment} is no longer running, as its state turned
     * out terminal when it woke. Ignored when the segment is not tracked.
     *
     * @param segment    the segment owning the instance
     * @param workflowId the id of the instance that turned out terminal
     */
    void recordRemoved(Segment segment, String workflowId) {
        withProjectionOf(segment, projection -> projection.recordRemoved(workflowId));
    }

    private void withProjectionOf(Segment segment, Consumer<SegmentProjection> action) {
        if (!indexed) {
            return;
        }
        SegmentProjection projection = segments.get(segment.getSegmentId());
        if (projection != null && projection.segment.equals(segment)) {
            action.accept(projection);
        }
    }

    /**
     * Stops tracking the given {@code segment}, and every tracked segment overlapping it, dropping changes that were
     * not written.
     * <p>
     * Dropped changes are not lost: no checkpoint stored a token beyond them, so the next claim streams them from the
     * event store again. A processor never holds two overlapping segments at once, so an overlapping segment that is
     * still tracked was split or merged without a release, and is tracked no longer.
     *
     * @param segment the segment to stop tracking
     */
    void discard(Segment segment) {
        segments.forEach((segmentId, projection) -> {
            if (WorkflowSegmentOwnership.overlap(projection.segment, segment)
                    && segments.remove(segmentId, projection)) {
                projection.close();
            }
        });
    }

    /**
     * Applies a handled event to the projection of the given {@code segment}.
     * <p>
     * Only lifecycle events of instances the segment owns change the projection, and any event of an owned instance
     * whose latest recorded change set a hint, which it clears; events published by a workflow through the publish
     * primitive never do. A segment that is not tracked, because its claim did not finish loading,
     * is ignored: its checkpoints fail, see {@link #decideCheckpoint(Segment)}.
     *
     * @param segment the segment handling the event, or {@code null} outside a segmented processor
     * @param event   the handled event
     */
    void apply(@Nullable Segment segment, EventMessage event) {
        if (segment == null) {
            return;
        }
        SegmentProjection projection = segments.get(segment.getSegmentId());
        if (projection != null) {
            projection.apply(event);
        }
    }

    /**
     * Decides the changes of the given {@code segment} its checkpoint writes: every change pending now. Returns the
     * flush writing exactly those, within the context it is given when the index can join its transaction.
     * <p>
     * A change applied after this decision is not written by the returned flush, but stays pending for a later
     * checkpoint or the release, also when it replaces a decided change of the same instance. A decided change that a
     * write made durable meanwhile is not written again. The flush waits for a write still in flight first, so writes
     * of a segment never overtake each other. It fails when the write fails, when it does not complete within the
     * checkpoint timeout, or when the segment is no longer tracked; the decided changes then stay pending. With
     * {@link WorkflowIndex#none()}, which keeps nothing, it completes right away.
     * <p>
     * Hints are the exception: a flush given a context also writes every hint recorded after this decision. Workflows
     * tend to hibernate exactly when they reach the safe point the checkpoint waits for, and a segment that turns idle
     * takes no later checkpoint to write their hints. That is safe, as the processor hands the segment no further
     * event while it waits for the checkpoint within that context: such a hint reflects no more events than one
     * recorded before the decision, and a claim ignores the hint of an instance with an event of its own after the
     * token, such as one appended by the instance itself.
     *
     * @param segment the segment whose checkpoint is being taken
     * @return the flush writing the decided changes, and completing once they are durable, or written within its
     * context
     * @throws IllegalStateException when the segment is not tracked, as its claim did not finish loading: its handled
     *                               events were then never applied
     */
    WorkflowEngineCheckpointingSupport.CheckpointFlush decideCheckpoint(Segment segment) {
        return decideFlush(segment, checkpointTimeout);
    }

    /**
     * Writes every pending change of the given {@code segment} ahead of its release, in a transaction of the index's
     * own, waiting for them at most the release timeout.
     * <p>
     * Handling has stopped, so no change is missed. The returned future fails when a write fails or does not complete
     * in time, after which the release does not advance the stored token. A write still in flight then lands later,
     * which is harmless, as entries ahead of the stored token converge. The write never joins the transaction the
     * processor releases the segment in: a failed write could make that transaction roll back, which would keep the
     * processor from releasing its claim.
     *
     * @param segment the segment being released
     * @return a future completing once every change applied so far is durable
     */
    CompletableFuture<Void> flushForRelease(Segment segment) {
        CompletableFuture<Void> flushed;
        try {
            flushed = decideFlush(segment, releaseTimeout).write(null);
        } catch (IllegalStateException e) {
            flushed = CompletableFuture.failedFuture(e);
        }
        return flushed.whenComplete((ignored, failure) -> {
            if (failure != null) {
                logger.info("Could not write the running workflows of segment {} on release; its stored token does "
                                    + "not advance.",
                            segment.getSegmentId(), failure);
            }
        });
    }

    /**
     * Decides to write every change of the given {@code segment} pending now, and returns the flush writing exactly
     * those, failing when it does not complete within the given {@code timeout}.
     *
     * @param segment the segment whose changes to write
     * @param timeout the time the returned flush waits for the write at most
     * @return the flush writing the decided changes
     * @throws IllegalStateException when the segment is not tracked
     */
    private WorkflowEngineCheckpointingSupport.CheckpointFlush decideFlush(Segment segment, Duration timeout) {
        if (!indexed) {
            return context -> CompletableFuture.completedFuture(null);
        }
        SegmentProjection projection = trackedProjection(segment);
        Map<String, Change> decided = projection.decide();
        return context -> projection.write(decided, context).orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private SegmentProjection trackedProjection(Segment segment) {
        SegmentProjection projection = segments.get(segment.getSegmentId());
        if (projection == null) {
            throw new IllegalStateException(
                    "The running workflows of segment " + segment.getSegmentId() + " are not tracked."
            );
        }
        return projection;
    }


    /**
     * A change to the index that is not durable yet.
     *
     * @param running     whether the instance is running after the latest change
     * @param hint        the hint the running instance hibernated with, or {@code null} for none
     * @param seq         the number of the latest change, telling whether a write covered it
     * @param baseWritten whether a write without the hint already wrote that the instance runs, so only a write within
     *                    the processor's transaction still has to write this change
     */
    private record Change(boolean running, @Nullable HibernationHint hint, long seq, boolean baseWritten) {

        boolean setsHint() {
            return running && hint != null;
        }

        Change withBaseWritten() {
            return new Change(running, hint, seq, true);
        }
    }

    /**
     * A write in flight: the changes it writes in full, those it writes without their hint, which stay pending, and
     * the future settling once the index settled it, whatever the outcome.
     */
    private record Write(Map<String, Change> changes,
                         Map<String, Change> withoutHint,
                         CompletableFuture<Void> settled) {

        /**
         * Returns the delta writing every change of this write, those without their hint as running without a hint.
         */
        IndexDelta delta() {
            Map<String, @Nullable HibernationHint> running = new HashMap<>();
            Set<String> removed = new HashSet<>();
            changes.forEach((id, change) -> {
                if (change.running()) {
                    running.put(id, change.hint());
                } else {
                    removed.add(id);
                }
            });
            withoutHint.keySet().forEach(id -> running.put(id, null));
            return new IndexDelta(running, removed);
        }

        boolean isEmpty() {
            return changes.isEmpty() && withoutHint.isEmpty();
        }
    }

    /**
     * The projection of one claimed segment. Updated from the handler thread and the workflow threads, and flushed
     * from the thread taking a checkpoint or releasing the segment, so all state is guarded by a lock. That is no
     * monitor: a workflow thread is a virtual thread, which keeps its carrier thread while it waits to enter a monitor.
     * Index writes are issued outside of the lock.
     */
    private final class SegmentProjection {

        private final ReentrantLock lock = new ReentrantLock();
        private final Segment segment;
        // Every change that is not durable, including those of the write in flight.
        private final Map<String, Change> unwritten = new HashMap<>();
        // The instances whose latest recorded change set a hint, or whose stored hint the claim adopted.
        private final Set<String> hinted = new HashSet<>();
        // The changes of each write within a context that has not ended yet, less those a later change replaced.
        private final List<Map<String, Change>> uncommitted = new ArrayList<>();
        private long changeCount;
        @Nullable
        private CompletableFuture<Void> writeInFlight;
        private boolean closed;

        private SegmentProjection(Segment segment, Map<String, Boolean> changes, Set<String> adoptedHints) {
            this.segment = segment;
            changes.forEach((id, running) -> record(id, running, null));
            hinted.addAll(adoptedHints);
        }

        private <T> T locked(Supplier<T> action) {
            lock.lock();
            try {
                return action.get();
            } finally {
                lock.unlock();
            }
        }

        private void locked(Runnable action) {
            lock.lock();
            try {
                action.run();
            } finally {
                lock.unlock();
            }
        }

        void apply(EventMessage event) {
            Metadata metadata = event.metadata();
            if (MetadataUtils.isPublishStep(metadata) || !MetadataUtils.hasWorkflowId().test(metadata)) {
                return;
            }
            String workflowId = MetadataUtils.getWorkflowId(metadata);
            if (!WorkflowSegmentOwnership.ownedBy(segment, workflowId)) {
                return;
            }
            Optional<Boolean> membership = membershipChange(metadata);
            locked(() -> {
                if (closed) {
                    return;
                }
                // Any event of an instance shows it no longer waits where it hibernated.
                if (hinted.contains(workflowId)) {
                    record(workflowId, true, null);
                }
                membership.ifPresent(isRunning -> record(workflowId, isRunning, null));
            });
        }

        void recordHint(String workflowId, HibernationHint hint) {
            locked(() -> {
                if (!closed) {
                    record(workflowId, true, hint);
                }
            });
        }

        void clearHint(String workflowId) {
            locked(() -> {
                if (!closed && hinted.contains(workflowId)) {
                    record(workflowId, true, null);
                }
            });
        }

        void recordRemoved(String workflowId) {
            locked(() -> {
                if (!closed) {
                    record(workflowId, false, null);
                }
            });
        }

        /**
         * Records a change. Must be called while holding the lock.
         */
        private void record(String workflowId, boolean isRunning, @Nullable HibernationHint hint) {
            uncommitted.forEach(changes -> changes.remove(workflowId));
            Change change = new Change(isRunning, isRunning ? hint : null, ++changeCount, false);
            unwritten.put(workflowId, change);
            if (change.setsHint()) {
                hinted.add(workflowId);
            } else {
                hinted.remove(workflowId);
            }
        }

        /**
         * Returns every pending change, as decided for a write.
         *
         * @return the pending changes, by workflow id
         * @throws IllegalStateException when the segment is no longer tracked
         */
        Map<String, Change> decide() {
            return locked(() -> {
                if (closed) {
                    throw noLongerTracked();
                }
                return Map.copyOf(unwritten);
            });
        }

        private IllegalStateException noLongerTracked() {
            return new IllegalStateException(
                    "The running workflows of segment " + segment.getSegmentId() + " are no longer tracked."
            );
        }

        /**
         * Writes the given decided changes that are not durable yet once the write in flight settled, within the given
         * {@code context} when the index can join its transaction, and returns a future completing once they are
         * durable, or written within that context, or failing when the write fails.
         * <p>
         * A decided change is written as decided, also when a later change of the same instance replaced it: that
         * later change stays pending. A decided change is skipped only when its instance has no pending change left,
         * as a write made it durable meanwhile. A write that lands late, after a later write of the same instance, can
         * thus leave an older entry in the index. That converges: an instance never runs again once it ended, and only
         * a write within the context of a checkpoint writes hints, so such an entry is at worst one of an instance that
         * ended, which the next claim removes, see {@link ClaimedIndex#withoutStale(Set)}, or one without the hint the
         * instance hibernated with, which only makes the next claim restore it into memory.
         * <p>
         * A write within a {@code context} also writes every hint that is pending now, also one recorded after the
         * changes were decided, see {@link RunningWorkflowsProjection#decideCheckpoint(Segment)}.
         * <p>
         * A write within the context runs on the calling thread and has settled when this returns, so a later write
         * never waits for it. Only writes in a transaction of their own are ever in flight. A write that has to wait
         * for one writes in a transaction of its own as well, as it then no longer runs on the calling thread, which
         * the context's transaction is bound to. Changes that set a hint are only written in full within the context;
         * any other write writes their instance as running without a hint, and keeps them pending.
         */
        CompletableFuture<Void> write(Map<String, Change> decided, @Nullable ProcessingContext context) {
            CompletableFuture<Void> inFlight;
            Map<String, Change> pending = new HashMap<>();
            CompletableFuture<Void> settled = new CompletableFuture<>();
            lock.lock();
            try {
                if (closed) {
                    return CompletableFuture.failedFuture(noLongerTracked());
                }
                inFlight = writeInFlight;
                if (inFlight == null) {
                    decided.forEach((id, change) -> {
                        Change current = unwritten.get(id);
                        if (current != null) {
                            pending.put(id, current.seq() == change.seq() ? current : change);
                        }
                    });
                    if (context != null) {
                        unwritten.forEach((id, current) -> {
                            if (current.setsHint()) {
                                pending.put(id, current);
                            }
                        });
                    }
                    if (!pending.isEmpty()) {
                        writeInFlight = settled;
                    }
                }
            } finally {
                lock.unlock();
            }
            if (inFlight != null) {
                return writeAfter(inFlight, decided);
            }
            return pending.isEmpty()
                    ? CompletableFuture.completedFuture(null)
                    : issue(Map.copyOf(pending), settled, context);
        }

        /**
         * Writes the given decided changes in a transaction of the index's own once the given write in flight settled,
         * whatever its outcome: the changes of a failed write are pending again, and written by this one.
         * <p>
         * Writes nothing when the returned future completed before the write in flight settled, as when its caller
         * timed it out: the caller gave up on the write, so the decided changes stay pending for a later write instead
         * of being written long after. A future composed on the write in flight would skip the write in that case as
         * well, as it does not run a function for a dependent that is already complete; this states it explicitly
         * instead of relying on that.
         */
        private CompletableFuture<Void> writeAfter(CompletableFuture<Void> inFlight, Map<String, Change> decided) {
            CompletableFuture<Void> chained = new CompletableFuture<>();
            inFlight.whenComplete((ignored, inFlightFailure) -> {
                // The caller gave up on this write, for example by timing out the returned future.
                if (chained.isDone()) {
                    return;
                }
                write(decided, null).whenComplete((written, failure) -> {
                    if (failure == null) {
                        chained.complete(null);
                    } else {
                        chained.completeExceptionally(failure);
                    }
                });
            });
            return chained;
        }

        /**
         * Issues a write of the given pending changes, and returns a future completing once it is durable, or failing
         * when it failed. The index bounds its own futures, so a write always settles, failing when it takes too long.
         * <p>
         * The returned future is a new one, completed after the write settled: a caller may time it out without
         * skipping the bookkeeping of the write, which a callback on a future the caller times out would skip.
         * <p>
         * With a {@code context}, the index may write within its transaction, which only becomes durable when the
         * context commits. The changes of a write that completed therefore become pending again when the context
         * fails, except those a later change replaced meanwhile. That is registered on the calling thread, while the
         * context surely still runs.
         */
        private CompletableFuture<Void> issue(Map<String, Change> pending,
                                              CompletableFuture<Void> settled,
                                              @Nullable ProcessingContext context) {
            Write write;
            CompletableFuture<Void> stored;
            Map<String, Change> restorable = null;
            try {
                Optional<CompletableFuture<Void>> joined = Optional.empty();
                Write full = new Write(pending, Map.of(), settled);
                if (context != null) {
                    restorable = trackUncommitted(pending);
                    joined = workflowIndex.storeWithin(indexName, full.delta(), context);
                }
                if (joined.isPresent()) {
                    write = full;
                    CompletableFuture<Void> written = joined.get();
                    Map<String, Change> restore = requireNonNull(restorable);
                    context.onError((failedContext, phase, error) -> {
                        // A write still in flight settles on its own; rewriting one that did not join is harmless.
                        if (written.isDone() && !written.isCompletedExceptionally()) {
                            restore(restore);
                        }
                    });
                    context.whenComplete(completedContext -> forget(restore));
                    stored = written;
                } else {
                    if (restorable != null) {
                        forget(restorable);
                    }
                    write = withoutHintSets(pending, settled);
                    stored = write.isEmpty()
                            ? CompletableFuture.completedFuture(null)
                            : workflowIndex.store(indexName, write.delta());
                }
            } catch (RuntimeException e) {
                if (restorable != null) {
                    // The write failed as a whole, which keeps its changes pending: nothing is left to restore.
                    forget(restorable);
                }
                write = new Write(pending, Map.of(), settled);
                stored = CompletableFuture.failedFuture(e);
            }
            CompletableFuture<Void> result = new CompletableFuture<>();
            Write issued = write;
            stored.whenComplete((ignored, failure) -> {
                onWriteSettled(issued, failure);
                if (failure == null) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(failure);
                }
            });
            return result;
        }

        /**
         * Returns a write of the given pending changes for a transaction of the index's own: a change that sets a
         * hint is written as running without the hint, unless an earlier such write already did, and stays pending.
         */
        private static Write withoutHintSets(Map<String, Change> pending, CompletableFuture<Void> settled) {
            Map<String, Change> changes = new HashMap<>();
            Map<String, Change> withoutHint = new HashMap<>();
            pending.forEach((id, change) -> {
                if (!change.setsHint()) {
                    changes.put(id, change);
                } else if (!change.baseWritten()) {
                    withoutHint.put(id, change);
                }
            });
            return new Write(changes, withoutHint, settled);
        }

        /**
         * Starts tracking the given changes of a write within a context, until that context ended. Only the changes
         * that are still the pending change of their instance are tracked: a rollback must not make a change pending
         * again that a later change replaced, as that later change may be durable by then.
         */
        private Map<String, Change> trackUncommitted(Map<String, Change> changes) {
            return locked(() -> {
                Map<String, Change> restorable = new HashMap<>();
                changes.forEach((id, change) -> {
                    Change current = unwritten.get(id);
                    if (current != null && current.seq() == change.seq()) {
                        restorable.put(id, change);
                    }
                });
                uncommitted.add(restorable);
                return restorable;
            });
        }

        /**
         * Makes the given changes of a write pending again, except those a later change of the same instance replaced,
         * see {@link #record(String, boolean, HibernationHint)}: the transaction the write joined did not commit.
         */
        private void restore(Map<String, Change> restorable) {
            locked(() -> {
                if (forget(restorable)) {
                    restorable.forEach(unwritten::putIfAbsent);
                }
            });
        }

        /**
         * Stops tracking the given changes of a write within a context, returning whether they were still tracked.
         */
        private boolean forget(Map<String, Change> restorable) {
            return locked(() -> uncommitted.removeIf(tracked -> tracked == restorable));
        }

        /**
         * Stops applying events and flushing. A write in flight still lands.
         */
        void close() {
            locked(() -> closed = true);
        }

        private void onWriteSettled(Write write, @Nullable Throwable failure) {
            locked(() -> {
                writeInFlight = null;
                if (failure == null) {
                    // A change applied after the write was decided stays pending.
                    write.changes().forEach((id, written) -> unwritten.computeIfPresent(
                            id, (ignored, current) -> current.seq() == written.seq() ? null : current
                    ));
                    // A hint written without its hint stays pending, until a write within a context writes it.
                    write.withoutHint().forEach((id, written) -> unwritten.computeIfPresent(
                            id,
                            (ignored, current) -> current.seq() == written.seq() ? current.withBaseWritten() : current
                    ));
                }
                // A failed write keeps its changes pending.
            });
            write.settled().complete(null);
            if (failure == null) {
                return;
            }
            Optional<String> invalidId = write.delta().added().stream()
                                              .filter(id -> !WorkflowIds.isValid(id))
                                              .findFirst();
            if (invalidId.isPresent()) {
                logger.error("Could not write the running workflows of segment {}, which contain the invalid "
                                     + "workflow id '{}', as {}. An index that rejects such ids, like the JPA index, "
                                     + "keeps the segment from making progress until that workflow is cancelled or "
                                     + "has ended, or the workflow index is disabled.",
                             segment.getSegmentId(),
                             WorkflowIds.abbreviate(invalidId.get()),
                             WorkflowIds.invalidity(invalidId.get()).orElseThrow(),
                             failure);
            } else {
                logger.warn("Could not write the running workflows of segment {}.", segment.getSegmentId(), failure);
            }
        }
    }
}
