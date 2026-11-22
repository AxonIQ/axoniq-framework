/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.Entry;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.runtime.util.WorkflowIds;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GapAwareTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.WrappedToken;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
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
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Loads, when a segment is claimed, which workflow instances the segment owns are running, and which changes the
 * {@link WorkflowIndex} does not hold yet.
 * <p>
 * The {@link WorkflowEngine} owns one loader and calls
 * {@link #load(Segment, TrackingToken, ProcessingContext, Duration)} on every claim, before it restores the running
 * instances. The result, a {@link ClaimedIndex}, is handed to
 * {@link RunningWorkflowsProjection#initialize(Segment, ClaimedIndex)}, which tracks the changes from there on and
 * writes them with the checkpoints of the segment.
 * <p>
 * With an initialized index, a claim combines the stored entries of the segment with the lifecycle events from the
 * token the segment resumes from, streamed from the event store, instead of reading every lifecycle event ever. When
 * the index was not initialized, it reads the full running-workflow list of the {@link WorkflowStore} instead, and
 * initializes the index with it. Whenever the index or the event store cannot be used, it falls back to that list as
 * well. Applying an ordered run of lifecycle events converges to the same running set whatever the stored entries
 * already reflect, so entries ahead of the token are harmless, see {@link RunningWorkflowsProjection}.
 * <p>
 * An entry may carry the {@link HibernationHint hint} that its instance hibernated. A claim streaming the events from
 * the token accepts a hint when none of those events belongs to the instance, and reports it in
 * {@link ClaimedIndex#hibernated()} rather than among the instances to restore: the instance cannot have moved on
 * without an event of its own. The claim clears every other hint it finds, through a pending change writing the
 * instance without a hint, as the instance is restored and may move on. Every fallback restores every instance, and
 * clears the hints of the stored entries as well.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
final class RunningWorkflowsClaimLoader {

    private static final Logger logger = LoggerFactory.getLogger(RunningWorkflowsClaimLoader.class);

    private final WorkflowIndex workflowIndex;
    // Whether the index keeps anything at all: WorkflowIndex.none() is bypassed entirely.
    private final boolean indexed;
    private final String indexName;
    private final WorkflowStore workflowStore;

    /**
     * Creates a loader reading the given workflow index.
     *
     * @param workflowIndex the index persisting the running workflows
     * @param indexName     the name scoping the entries in the {@code workflowIndex}
     * @param workflowStore the store providing the full running-workflow list when the index cannot be used
     */
    RunningWorkflowsClaimLoader(WorkflowIndex workflowIndex, String indexName, WorkflowStore workflowStore) {
        this.workflowIndex = requireNonNull(workflowIndex, "The WorkflowIndex must not be null.");
        this.indexed = workflowIndex != WorkflowIndex.none();
        this.indexName = requireNonNull(indexName, "The index name must not be null.");
        this.workflowStore = requireNonNull(workflowStore, "The WorkflowStore must not be null.");
    }

    /**
     * Loads the running workflow instances owned by the given {@code segment}, for restoring them on claim.
     * <p>
     * With an initialized index, the result is the stored entries of the segment plus every lifecycle event from
     * {@code claimedFrom} on, streamed from the {@link EventStore} of the {@code sourcingContext} up to the latest
     * token taken before opening the stream. Without a token to stream from, without an event store to stream from, or
     * when streaming fails or does not reach that token within {@code timeout}, the result is the full running-workflow
     * list of the {@link WorkflowStore} instead. Either way it reflects the running set at the head of the stream.
     * <p>
     * When the index was not initialized, the result is the full running-workflow list, and the index is initialized
     * with the running instances of every segment. When the index cannot be loaded, or cannot be initialized, within
     * {@code timeout}, the result is the full running-workflow list as well, and every instance in it becomes a pending
     * addition. The index then may keep entries of instances that are no longer running, which a later claim removes
     * once it finds their sourced state terminal, but it never lacks a running instance.
     * <p>
     * With {@link WorkflowIndex#none()}, the result is the full running-workflow list, without changes to write.
     *
     * @param segment         the claimed segment
     * @param claimedFrom     the stored token the segment resumes from, or {@code null} when it has consumed nothing
     * @param sourcingContext the context of the claim, providing the event store
     * @param timeout         the bound on loading or initializing the index and on streaming the events after the token
     * @return a future completing with the running instances and the changes the index does not hold yet
     */
    CompletableFuture<ClaimedIndex> load(Segment segment,
                                         @Nullable TrackingToken claimedFrom,
                                         ProcessingContext sourcingContext,
                                         Duration timeout) {
        if (!indexed) {
            // Nothing to write: the index keeps nothing.
            return loadFromWorkflowStore(segment, StoredEntries.NONE, sourcingContext)
                    .thenApply(running -> new ClaimedIndex(running.runningWorkflowIds(), Map.of()));
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        return bounded(() -> workflowIndex.load(indexName, segment), timeout).handle((stored, failure) -> {
            if (failure != null) {
                logger.warn("Could not load the running-workflow index of segment {}; loading its running workflows "
                                    + "from the full running-workflow list instead.",
                            segment.getSegmentId(), failure);
                // Compared to an empty index, so every running instance is written, and no entry is removed.
                return loadFromWorkflowStore(segment, StoredEntries.NONE, sourcingContext);
            }
            if (stored.isEmpty()) {
                logger.info("The running-workflow index [{}] is not initialized; loading the running workflows of "
                                    + "segment {} from the full running-workflow list and initializing the index.",
                            indexName, segment.getSegmentId());
                return initializeFromWorkflowStore(segment, sourcingContext, deadline);
            }
            return loadFromIndex(segment, StoredEntries.of(stored.get()), claimedFrom, sourcingContext,
                                 remaining(deadline));
        }).thenCompose(Function.identity());
    }

    /**
     * Returns the time left until the given {@code deadline}, taken from {@link System#nanoTime()}, never negative.
     */
    private static Duration remaining(long deadline) {
        return Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
    }

    /**
     * Calls the given index operation, bounded by the given {@code timeout}.
     */
    private static <T> CompletableFuture<T> bounded(Supplier<CompletableFuture<T>> operation, Duration timeout) {
        CompletableFuture<T> result;
        try {
            result = operation.get();
        } catch (RuntimeException e) {
            result = CompletableFuture.failedFuture(e);
        }
        // A copy, so the timeout does not complete a future the index owns.
        return result.copy().orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<ClaimedIndex> loadFromIndex(Segment segment,
                                                          StoredEntries stored,
                                                          @Nullable TrackingToken claimedFrom,
                                                          ProcessingContext sourcingContext,
                                                          Duration tailTimeout) {
        StreamableEventSource eventSource = claimedFrom == null ? null : eventSourceOf(sourcingContext);
        if (claimedFrom == null || eventSource == null) {
            // Without a token, the lifecycle events to read are every lifecycle event ever, which the full
            // running-workflow list never costs more than.
            return loadFromWorkflowStore(segment, stored, sourcingContext);
        }
        return readTailFrom(eventSource, claimedFrom, sourcingContext, tailTimeout)
                .thenApply(tail -> {
                    logger.debug("Loaded the running workflows of segment {} from {} index entries and {} lifecycle "
                                         + "events after {}.",
                                 segment.getSegmentId(), stored.ids().size(), tail.lifecycleEvents().size(),
                                 claimedFrom);
                    return applyTail(segment, stored, tail);
                })
                .exceptionallyCompose(failure -> {
                    logger.warn("Could not stream the lifecycle events of segment {} from {}; loading its running "
                                        + "workflows from the full running-workflow list instead.",
                                segment.getSegmentId(), claimedFrom, failure);
                    return loadFromWorkflowStore(segment, stored, sourcingContext);
                });
    }

    @Nullable
    private static StreamableEventSource eventSourceOf(ProcessingContext context) {
        try {
            return context.component(EventStore.class);
        } catch (ComponentNotFoundException e) {
            return null;
        }
    }

    private CompletableFuture<ClaimedIndex> loadFromWorkflowStore(Segment segment,
                                                                  StoredEntries stored,
                                                                  ProcessingContext sourcingContext) {
        return workflowStore.loadRunningWorkflows(sourcingContext).thenApply(running -> {
            Set<String> owned = ownedBy(segment, running.workflowIds());
            logger.debug("Loaded the {} running workflows of segment {} from the full running-workflow list.",
                         owned.size(), segment.getSegmentId());
            return compare(segment, owned, stored);
        });
    }

    /**
     * Loads the full running-workflow list and initializes the index with the running instances of every segment.
     * When the index cannot be initialized, every running instance of the segment becomes a pending addition instead,
     * and a later claim tries to initialize the index again. While a running instance has an invalid id that the index
     * rejects, every such attempt fails, so the index stays uninitialized until that instance is cancelled or has
     * ended.
     */
    private CompletableFuture<ClaimedIndex> initializeFromWorkflowStore(Segment segment,
                                                                        ProcessingContext sourcingContext,
                                                                        long deadline) {
        return workflowStore.loadRunningWorkflows(sourcingContext).thenCompose(running -> {
            Set<String> all = Set.copyOf(running.workflowIds());
            Set<String> owned = ownedBy(segment, all);
            return bounded(() -> workflowIndex.initialize(indexName, all), remaining(deadline))
                    .handle((initialized, failure) -> {
                        if (failure != null) {
                            logInitializationFailure(all, failure);
                            return compare(segment, owned, StoredEntries.NONE);
                        }
                        return new ClaimedIndex(owned, Map.of());
                    });
        });
    }

    private void logInitializationFailure(Set<String> running, Throwable failure) {
        Optional<String> invalidId = running.stream().filter(id -> !WorkflowIds.isValid(id)).findFirst();
        if (invalidId.isPresent()) {
            logger.error("Could not initialize the running-workflow index [{}], whose running workflows contain "
                                 + "the invalid workflow id '{}', as {}. An index that rejects such ids, like the JPA "
                                 + "index, stays uninitialized until that workflow is cancelled or has ended: every "
                                 + "claim reads the full running-workflow list, and the segment owning that workflow "
                                 + "makes no progress.",
                         indexName,
                         WorkflowIds.abbreviate(invalidId.get()),
                         WorkflowIds.invalidity(invalidId.get()).orElseThrow(),
                         failure);
        } else {
            logger.warn("Could not initialize the running-workflow index [{}]; a later claim initializes it.",
                        indexName, failure);
        }
    }

    private static Set<String> ownedBy(Segment segment, Set<String> workflowIds) {
        return workflowIds.stream()
                          .filter(id -> WorkflowSegmentOwnership.ownedBy(segment, id))
                          .collect(Collectors.toSet());
    }

    /**
     * Returns the given running instances of the segment, with the changes that turn the {@code stored} entries into
     * them. Every running instance is restored, so the changes clear the hints of the stored entries.
     */
    private static ClaimedIndex compare(Segment segment, Set<String> owned, StoredEntries stored) {
        Map<String, Boolean> changes = new HashMap<>();
        owned.stream()
             .filter(id -> !stored.ids().contains(id) || stored.hints().containsKey(id))
             .forEach(id -> changes.put(id, true));
        stored.ids().stream()
              .filter(id -> !owned.contains(id) && WorkflowSegmentOwnership.ownedBy(segment, id))
              .forEach(id -> changes.put(id, false));
        return new ClaimedIndex(owned, changes);
    }

    /**
     * Reads the lifecycle events, and the ids of the instances with any event, after the given {@code position} up to
     * the head of the stream.
     */
    private static CompletableFuture<Tail> readTailFrom(StreamableEventSource eventSource,
                                                        TrackingToken position,
                                                        ProcessingContext context,
                                                        Duration timeout) {
        // The token may be wrapped, for example during a replay or a merge, which an event store does not stream
        // from. The lower bound only ever streams more events, which re-applying makes harmless.
        TrackingToken from = WrappedToken.unwrapLowerBound(position);
        var reader = new TailReader();
        return eventSource.latestToken(context)
                          .thenCompose(head -> {
                              // No head means an empty stream, and a head the position covers means nothing was
                              // appended after it. Reaching the head by position is not enough: an event filling a gap
                              // of the position may have been committed since, and handled by another claim.
                              if (head == null || from.covers(head)) {
                                  return CompletableFuture.completedFuture(Tail.EMPTY);
                              }
                              return reader.read(eventSource.open(StreamingCondition.startingFrom(withoutGaps(from)),
                                                                  context),
                                                 head);
                          })
                          .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                          .whenComplete((events, failure) -> reader.close());
    }

    /**
     * Returns the position to stream the index tail from: the given {@code position}, or for a gap-aware position with
     * gaps, the position just before its first gap, without any gaps.
     * <p>
     * Streaming from a position with gaps delivers the events filling those gaps first, but a gap that stays open, for
     * example of a transaction that rolled back, delivers nothing and is never reported as such. Reading would then
     * wait for the timeout whenever nothing was appended after the position, and fall back to the full running-workflow
     * list. Streaming from before the first gap delivers every event from there in order instead, including the one at
     * the position itself, so the tail always reaches the head. The events between the first gap and the position are
     * read once more, which is harmless as re-applying them converges to the same set. Gaps are bounded by the event
     * store's maximum gap offset, so this rereads a bounded number of events.
     */
    private static TrackingToken withoutGaps(TrackingToken position) {
        if (position instanceof GapAwareTrackingToken gapAware && gapAware.hasGaps()) {
            return GapAwareTrackingToken.newInstance(gapAware.getGaps().first() - 1, List.of());
        }
        return position;
    }

    private static ClaimedIndex applyTail(Segment segment, StoredEntries stored, Tail tail) {
        Map<String, Boolean> running = new HashMap<>();
        stored.ids().forEach(id -> running.put(id, true));
        for (EventMessage event : tail.lifecycleEvents()) {
            Metadata metadata = event.metadata();
            RunningWorkflowsProjection.membershipChange(metadata).ifPresent(isRunning -> {
                String workflowId = MetadataUtils.getWorkflowId(metadata);
                if (WorkflowSegmentOwnership.ownedBy(segment, workflowId)) {
                    running.put(workflowId, isRunning);
                }
            });
        }
        Set<String> runningIds = new HashSet<>();
        Map<String, HibernationHint> hibernated = new HashMap<>();
        Map<String, Boolean> changes = new HashMap<>();
        running.forEach((id, isRunning) -> {
            HibernationHint hint = stored.hints().get(id);
            if (isRunning && hint != null && !tail.workflowIdsWithEvents().contains(id)) {
                // Nothing happened to the instance since it hibernated: it stays out of memory.
                hibernated.put(id, hint);
                return;
            }
            if (isRunning) {
                runningIds.add(id);
            }
            if (isRunning != stored.ids().contains(id) || (isRunning && hint != null)) {
                // Also clears the hint of an instance that is restored, as it may move on.
                changes.put(id, isRunning);
            }
        });
        return new ClaimedIndex(runningIds, hibernated, changes);
    }

    /**
     * The entries a claim loaded from the index: their ids, and the hints of those that carry one.
     *
     * @param ids   the ids of the stored entries
     * @param hints the hints of the stored entries that carry one, by workflow id
     */
    private record StoredEntries(Set<String> ids, Map<String, HibernationHint> hints) {

        static final StoredEntries NONE = new StoredEntries(Set.of(), Map.of());

        static StoredEntries of(Set<Entry> entries) {
            Set<String> ids = new HashSet<>();
            Map<String, HibernationHint> hints = new HashMap<>();
            for (Entry entry : entries) {
                ids.add(entry.workflowId());
                if (entry.hint() != null) {
                    hints.put(entry.workflowId(), entry.hint());
                }
            }
            return new StoredEntries(Set.copyOf(ids), Map.copyOf(hints));
        }
    }

    /**
     * What a claim read after the token it resumes from.
     *
     * @param lifecycleEvents       the events that change whether an instance runs, in stream order
     * @param workflowIdsWithEvents the ids of the instances with any event of their own, lifecycle or not
     */
    private record Tail(List<EventMessage> lifecycleEvents, Set<String> workflowIdsWithEvents) {

        static final Tail EMPTY = new Tail(List.of(), Set.of());
    }

    /**
     * What a claim loaded for a segment.
     *
     * @param runningWorkflowIds the running workflow instances owned by the segment, to restore
     * @param hibernated         the hints of the running instances owned by the segment that stay out of memory, by
     *                           workflow id; never among the {@code runningWorkflowIds}
     * @param changes            the membership changes, by workflow id, that the workflow index does not hold yet; a
     *                           running instance is written without a hint
     */
    record ClaimedIndex(Set<String> runningWorkflowIds,
                        Map<String, HibernationHint> hibernated,
                        Map<String, Boolean> changes) {

        ClaimedIndex {
            runningWorkflowIds = Set.copyOf(runningWorkflowIds);
            hibernated = Map.copyOf(hibernated);
            changes = Map.copyOf(changes);
        }

        /**
         * Creates a result without hibernated instances.
         *
         * @param runningWorkflowIds the running workflow instances owned by the segment, to restore
         * @param changes            the membership changes, by workflow id, that the workflow index does not hold yet
         */
        ClaimedIndex(Set<String> runningWorkflowIds, Map<String, Boolean> changes) {
            this(runningWorkflowIds, Map.of(), changes);
        }

        /**
         * Returns this result with the hibernated instances among those to restore, and their hints cleared: for a
         * claim that does not leave any instance out of memory, as hibernation is not enabled.
         *
         * @return this result restoring every running instance
         */
        ClaimedIndex restoringHibernated() {
            if (hibernated.isEmpty()) {
                return this;
            }
            Set<String> running = new HashSet<>(runningWorkflowIds);
            running.addAll(hibernated.keySet());
            Map<String, Boolean> cleared = new HashMap<>(changes);
            hibernated.keySet().forEach(id -> cleared.put(id, true));
            return new ClaimedIndex(running, Map.of(), cleared);
        }

        /**
         * Returns this result without the given {@code stale} instances: instances the index lists as running, but
         * whose sourced state turned out terminal when the claim restored them.
         * <p>
         * Their removal becomes a pending change, so the first flush deletes their entries. That heals an entry a late
         * write left behind after the index already recorded the instance's end, which the events from the stored
         * token never remove again. The removal is safe whatever token it is written at: a terminal state is final,
         * as the instance ignores every later lifecycle event. A lifecycle event handled after the claim still
         * overrides the removal, as it is applied to the pending changes after it.
         *
         * @param stale the ids of the instances that turned out not to run
         * @return this result without the given instances
         */
        ClaimedIndex withoutStale(Set<String> stale) {
            if (stale.isEmpty()) {
                return this;
            }
            Set<String> running = new HashSet<>(runningWorkflowIds);
            running.removeAll(stale);
            Map<String, Boolean> healed = new HashMap<>(changes);
            stale.forEach(id -> healed.put(id, false));
            return new ClaimedIndex(running, hibernated, healed);
        }
    }

    /**
     * Reads an event stream up to a head token taken before the stream was opened, keeping only the events that change
     * whether a workflow instance runs, and the ids of the instances with any event of their own.
     * <p>
     * The stream is not filtered by criteria: every event delivers its token, so reading can stop at the first token
     * covering the head, even when the last lifecycle event lies before it. Reading never stops merely because nothing
     * is available yet, as a remote stream delivers asynchronously; it continues on the stream's availability callback.
     * It ends early only when the stream completes or fails. The caller bounds the read and closes the reader.
     */
    private static final class TailReader {

        private final List<EventMessage> events = new ArrayList<>();
        private final Set<String> workflowIdsWithEvents = new HashSet<>();
        private final CompletableFuture<Tail> result = new CompletableFuture<>();
        @Nullable
        private volatile MessageStream<EventMessage> stream;
        @Nullable
        private TrackingToken head;

        CompletableFuture<Tail> read(MessageStream<EventMessage> stream, TrackingToken head) {
            synchronized (this) {
                this.stream = stream;
                this.head = head;
            }
            if (result.isDone()) {
                // Closed before the stream was opened.
                stream.close();
                return result;
            }
            try {
                stream.setCallback(this::drain);
                drain();
            } catch (RuntimeException e) {
                result.completeExceptionally(e);
            }
            return result;
        }

        /**
         * Returns whether the given streamed {@code token} reached the {@code head}.
         * <p>
         * Compared by position when both tokens expose one. A token can then reach the head without covering it: a
         * gap-aware token streamed past a gap below the head does not cover a head without that gap. The stream starts
         * without gaps and delivers in order, so such a gap holds an event that was not visible when the stream read
         * past it, nor when the head was taken before that. The stored token of the claimed segment, which the
         * processor resumes from, was taken before as well, so it does not cover that event either: handling applies
         * it to the index later. Tokens without a position fall back to {@link TrackingToken#covers(TrackingToken)}.
         */
        static boolean reaches(TrackingToken token, TrackingToken head) {
            OptionalLong tokenPosition = token.position();
            OptionalLong headPosition = head.position();
            if (tokenPosition.isPresent() && headPosition.isPresent()) {
                return tokenPosition.getAsLong() >= headPosition.getAsLong();
            }
            return token.covers(head);
        }

        void close() {
            result.completeExceptionally(new IllegalStateException("The lifecycle event stream was closed."));
            MessageStream<EventMessage> current = stream;
            if (current != null) {
                current.close();
            }
        }

        private void complete() {
            result.complete(new Tail(List.copyOf(events), Set.copyOf(workflowIdsWithEvents)));
        }

        private synchronized void drain() {
            MessageStream<EventMessage> current = stream;
            if (current == null || head == null) {
                return;
            }
            try {
                while (!result.isDone()) {
                    Optional<Throwable> error = current.error();
                    if (error.isPresent()) {
                        result.completeExceptionally(error.get());
                        return;
                    }
                    Optional<MessageStream.Entry<EventMessage>> entry = current.next();
                    if (entry.isEmpty()) {
                        if (current.isCompleted()) {
                            // The stream ended before reaching the head: it delivered everything there is.
                            complete();
                        }
                        return;
                    }
                    EventMessage event = entry.get().message();
                    Metadata metadata = event.metadata();
                    if (RunningWorkflowsProjection.membershipChange(metadata).isPresent()) {
                        events.add(event);
                    }
                    if (!MetadataUtils.isPublishStep(metadata) && MetadataUtils.hasWorkflowId().test(metadata)) {
                        workflowIdsWithEvents.add(MetadataUtils.getWorkflowId(metadata));
                    }
                    TrackingToken token = TrackingToken.fromContext(entry.get()).orElse(null);
                    if (token != null && reaches(token, head)) {
                        complete();
                    }
                }
            } catch (RuntimeException e) {
                result.completeExceptionally(e);
            }
        }
    }
}
