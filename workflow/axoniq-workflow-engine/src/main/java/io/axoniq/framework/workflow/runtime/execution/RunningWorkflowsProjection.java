/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.index.api.WorkflowIndex;
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
 *     <li>Every checkpoint of the segment first writes every pending change through
 *     {@link #flushForCheckpoint(Segment, ProcessingContext)}, and fails when that write fails or does not complete in
 *     time.</li>
 *     <li>On release, {@link #flushForRelease(Segment)} writes the pending changes within the release timeout, in a
 *     transaction of the index's own, and {@link #discard(Segment)} stops tracking the segment.</li>
 * </ul>
 * A checkpoint write gets the context in which the processor stores the token, so an index in the same database can
 * write within that transaction, see {@link WorkflowIndex#store(String, IndexDelta, ProcessingContext)}. Such a write
 * is durable only once the context commits: when the context fails instead, its changes become pending again, unless
 * a later change of the same instance replaced them.
 *
 * <h2>Why a stored token never passes an unwritten change</h2>
 * The processor stores a token of a segment that handled events only after a checkpoint, and every checkpoint writes
 * every change applied so far first. So once a token is stored, every change that an event up to that token caused is
 * durable. A checkpoint whose write fails fails as well: the processor releases the segment and claims it again, and
 * the claim restores the running instances from the stored entries and the lifecycle events after the stored token.
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
            segments.put(segment.getSegmentId(), new SegmentProjection(segment, claimed.changes()));
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
     * Only lifecycle events of instances the segment owns change the projection; events published by a workflow
     * through the publish primitive never do. A segment that is not tracked, because its claim did not finish loading,
     * is ignored: its checkpoints fail, see {@link #flushForCheckpoint(Segment, ProcessingContext)}.
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
     * Writes every pending change of the given {@code segment} ahead of its checkpoint, within the given
     * {@code context} when the index can join its transaction.
     * <p>
     * Waits for a write still in flight first, so writes of a segment never overtake each other. The returned future
     * fails when a write fails, when the writes do not complete within the checkpoint timeout, or when the segment is
     * not tracked, as its claim did not finish loading: its handled events were then never applied. With
     * {@link WorkflowIndex#none()}, which keeps nothing, it completes right away.
     *
     * @param segment the segment whose checkpoint is being taken
     * @param context the context the processor stores the checkpoint in, or {@code null} when unknown
     * @return a future completing once every change applied so far is durable, or written within the {@code context}
     */
    CompletableFuture<Void> flushForCheckpoint(Segment segment, @Nullable ProcessingContext context) {
        return flushWithin(segment, context, checkpointTimeout);
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
        return flushWithin(segment, null, releaseTimeout).whenComplete((flushed, failure) -> {
            if (failure != null) {
                logger.info("Could not write the running workflows of segment {} on release; its stored token does "
                                    + "not advance.",
                            segment.getSegmentId(), failure);
            }
        });
    }

    private CompletableFuture<Void> flushWithin(Segment segment,
                                                @Nullable ProcessingContext context,
                                                Duration timeout) {
        if (!indexed) {
            return CompletableFuture.completedFuture(null);
        }
        SegmentProjection projection = segments.get(segment.getSegmentId());
        if (projection == null) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "The running workflows of segment " + segment.getSegmentId() + " are not tracked."
            ));
        }
        return projection.flush(context).orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }


    /**
     * A change to the index that is not durable yet.
     *
     * @param running whether the instance is running after the latest change
     * @param seq     the number of the latest change, telling whether a write covered it
     */
    private record Change(boolean running, long seq) {

    }

    /**
     * A write in flight: the changes it carries, by workflow id, and the future settling once the index settled it,
     * whatever the outcome.
     */
    private record Write(Map<String, Change> changes, CompletableFuture<Void> settled) {

        IndexDelta delta() {
            Set<String> added = new HashSet<>();
            Set<String> removed = new HashSet<>();
            changes.forEach((id, change) -> (change.running() ? added : removed).add(id));
            return new IndexDelta(added, removed);
        }
    }

    /**
     * The projection of one claimed segment. Updated from the handler thread and flushed from the thread taking a
     * checkpoint or releasing the segment, so all state is guarded by this object's monitor. Index writes are issued
     * outside of it.
     */
    private final class SegmentProjection {

        private final Segment segment;
        // Every change that is not durable, including those of the write in flight.
        private final Map<String, Change> unwritten = new HashMap<>();
        // The changes of each write within a context that has not ended yet, less those a later change replaced.
        private final List<Map<String, Change>> uncommitted = new ArrayList<>();
        private long changeCount;
        @Nullable
        private Write writeInFlight;
        private boolean closed;

        private SegmentProjection(Segment segment, Map<String, Boolean> changes) {
            this.segment = segment;
            changes.forEach(this::record);
        }

        synchronized void apply(EventMessage event) {
            if (closed) {
                return;
            }
            Metadata metadata = event.metadata();
            membershipChange(metadata).ifPresent(isRunning -> {
                String workflowId = MetadataUtils.getWorkflowId(metadata);
                if (WorkflowSegmentOwnership.ownedBy(segment, workflowId)) {
                    record(workflowId, isRunning);
                }
            });
        }

        /**
         * Records a change. Must be called while holding this object's monitor.
         */
        private void record(String workflowId, boolean isRunning) {
            uncommitted.forEach(changes -> changes.remove(workflowId));
            unwritten.put(workflowId, new Change(isRunning, ++changeCount));
        }

        /**
         * Writes every pending change once the write in flight settled, within the given {@code context} when the
         * index can join its transaction, and returns a future completing once they are durable, or written within
         * that context, or failing when the write fails.
         * <p>
         * A write within the context runs on the calling thread and has settled when this returns, so a later flush
         * never waits for it. Only writes in a transaction of their own are ever in flight. A flush that has to wait
         * for one writes in a transaction of its own as well, as it then no longer runs on the calling thread, which
         * the context's transaction is bound to.
         */
        CompletableFuture<Void> flush(@Nullable ProcessingContext context) {
            Write write;
            CompletableFuture<Void> inFlight;
            synchronized (this) {
                if (closed) {
                    return CompletableFuture.failedFuture(new IllegalStateException(
                            "The running workflows of segment " + segment.getSegmentId() + " are no longer tracked."
                    ));
                }
                inFlight = writeInFlight == null ? null : writeInFlight.settled();
                write = inFlight == null ? prepareWrite() : null;
            }
            if (inFlight != null) {
                // Whatever its outcome: the changes of a failed write are pending again, and written next.
                return inFlight.thenCompose(settled -> flush(null));
            }
            return write == null ? CompletableFuture.completedFuture(null) : issue(write, context);
        }

        /**
         * Takes every pending change into a new write, or returns {@code null} when there are none. Must be called
         * while holding this object's monitor, with no write in flight.
         */
        @Nullable
        private Write prepareWrite() {
            if (unwritten.isEmpty()) {
                return null;
            }
            Write write = new Write(Map.copyOf(unwritten), new CompletableFuture<>());
            writeInFlight = write;
            return write;
        }

        /**
         * Issues the given write, and returns a future completing once it is durable, or failing when it failed. The
         * index bounds its own futures, so a write always settles, failing when it takes too long.
         * <p>
         * The returned future is a new one, completed after the write settled: a caller may time it out without
         * skipping the bookkeeping of the write, which a callback on a future the caller times out would skip.
         * <p>
         * With a {@code context}, the index may write within its transaction, which only becomes durable when the
         * context commits. The changes of a write that completed therefore become pending again when the context
         * fails, except those a later change replaced meanwhile. That is registered on the calling thread, while the
         * context surely still runs.
         */
        private CompletableFuture<Void> issue(Write write, @Nullable ProcessingContext context) {
            CompletableFuture<Void> stored;
            try {
                if (context == null) {
                    stored = workflowIndex.store(indexName, write.delta());
                } else {
                    Map<String, Change> restorable = trackUncommitted(write);
                    CompletableFuture<Void> written = workflowIndex.store(indexName, write.delta(), context);
                    context.onError((failedContext, phase, error) -> {
                        // A write still in flight settles on its own; rewriting one that did not join is harmless.
                        if (written.isDone() && !written.isCompletedExceptionally()) {
                            restore(restorable);
                        }
                    });
                    context.whenComplete(completedContext -> forget(restorable));
                    stored = written;
                }
            } catch (RuntimeException e) {
                stored = CompletableFuture.failedFuture(e);
            }
            CompletableFuture<Void> result = new CompletableFuture<>();
            stored.whenComplete((ignored, failure) -> {
                onWriteSettled(write, failure);
                if (failure == null) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(failure);
                }
            });
            return result;
        }

        /**
         * Starts tracking the changes of the given write within a context, until that context ended.
         */
        private synchronized Map<String, Change> trackUncommitted(Write write) {
            Map<String, Change> restorable = new HashMap<>(write.changes());
            uncommitted.add(restorable);
            return restorable;
        }

        /**
         * Makes the given changes of a write pending again, except those a later change of the same instance replaced,
         * see {@link #record(String, boolean)}: the transaction the write joined did not commit.
         */
        private synchronized void restore(Map<String, Change> restorable) {
            if (forget(restorable)) {
                restorable.forEach(unwritten::putIfAbsent);
            }
        }

        /**
         * Stops tracking the given changes of a write within a context, returning whether they were still tracked.
         */
        private synchronized boolean forget(Map<String, Change> restorable) {
            return uncommitted.removeIf(tracked -> tracked == restorable);
        }

        /**
         * Stops applying events and flushing. A write in flight still lands.
         */
        synchronized void close() {
            closed = true;
        }

        private void onWriteSettled(Write write, @Nullable Throwable failure) {
            synchronized (this) {
                writeInFlight = null;
                if (failure == null) {
                    // A change applied after the write was taken stays pending.
                    write.changes().forEach((id, written) -> unwritten.remove(id, written));
                }
                // A failed write keeps its changes pending.
            }
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
