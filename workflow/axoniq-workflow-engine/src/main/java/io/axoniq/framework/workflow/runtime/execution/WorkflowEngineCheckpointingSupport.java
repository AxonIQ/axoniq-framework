/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.messaging.eventstreaming.checkpoint.CheckpointTrigger;
import io.axoniq.framework.messaging.eventstreaming.checkpoint.Checkpointing;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.axonframework.common.BuilderUtils.assertPositive;

/**
 * A {@link Checkpointing} implementation dedicated for a {@link WorkflowEngine}, to separate check pointing behavior
 * from it.
 * <p>
 * This class has one job: coordinate checkpoint requests and complete
 * {@link #onCheckpointAdvanced(Segment, TrackingToken)} only after the host confirms that workflow-owned asynchronous
 * work is safe. It does not track processor progress or decide when the engine should switch to live mode.
 * <p>
 * Everything here is kept <em>per segment</em>. The processor hands out one {@link CheckpointTrigger} per claimed
 * segment, a request through it advances only that segment's stored token, and it is inert once that claim ends.
 * <p>
 * The trigger map is concurrent, and its updates are deliberately small: they never invoke processor or workflow
 * callbacks while a claim is being recorded or dropped. The coordinator callback supplied to
 * {@link #onCheckpointAdvanced(Segment, TrackingToken)} re-enters the support only after all the
 * {@link WorkflowExecution} latches (attached by the {@link CheckpointLatchCoordinator}) have been crossed.
 * <p>
 * A checkpoint also makes the state the coordinator derives from the handled events durable, such as the index of
 * running workflows: {@link #onCheckpointAdvanced(Segment, TrackingToken, ProcessingContext)} first runs
 * {@link CheckpointLatchCoordinator#flushCheckpoint(Segment, ProcessingContext)}, within the context the processor
 * stores the token in when it hands one over. When that flush fails, the checkpoint fails right away, and the processor
 * releases the segment and claims it again after a back-off, restoring its workflows from the event store.
 * <p>
 * A positive checkpoint interval forwards at most one checkpoint request per segment within that interval, so the
 * processor waits for safe points less often under load. A request within the interval is dropped, so the stored token
 * lags by up to the events of one interval, and those events are processed again after a crash. A segment that goes
 * idle keeps that lag until its next event or its release.
 *
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 5.4.0
 */
@Internal
public class WorkflowEngineCheckpointingSupport implements Checkpointing {

    private final CheckpointLatchCoordinator checkpointLatchCoordinator;
    private final Map<Integer, CheckpointTrigger> segmentIdToTrigger = new ConcurrentHashMap<>();
    private final long checkpointIntervalNanos;
    private final Map<Integer, Long> lastForwardNanos = new ConcurrentHashMap<>();
    // Segments whose release failed, until they are claimed again.
    private final Set<Integer> failedReleases = ConcurrentHashMap.newKeySet();

    /**
     * Creates checkpointing support for a {@link WorkflowEngine} that forwards at most one checkpoint request per
     * segment within the given {@code checkpointInterval}.
     * <p>
     * Every forwarded request makes the event processor wait, at its next batch commit, until all workflows of the
     * segment reached a safe point. A request that is not forwarded is dropped, not deferred: the stored token stays
     * at the last forwarded request until the next one or the release of the segment. So the stored token lags by up
     * to the events of one interval, and those events are processed again after a crash. A segment that goes idle
     * keeps that lag until its next event or its release.
     *
     * @param checkpointLatchCoordinator adds a latch to coordinated {@link WorkflowExecution WorkflowExecutions} to
     *                                   ensure all have reached a safe point to advance the checkpoint
     * @param checkpointInterval         the minimum time, in milliseconds, between two forwarded checkpoint requests
     *                                   of the same segment, where {@code 0} forwards every request
     * @throws AxonConfigurationException if {@code checkpointInterval} is negative
     */
    public WorkflowEngineCheckpointingSupport(CheckpointLatchCoordinator checkpointLatchCoordinator,
                                              long checkpointInterval) {
        this.checkpointLatchCoordinator = requireNonNull(
                checkpointLatchCoordinator, "The CheckpointLatchCoordinator must not be null."
        );
        assertPositive(checkpointInterval, "The checkpoint interval must not be negative.");
        this.checkpointIntervalNanos = TimeUnit.MILLISECONDS.toNanos(checkpointInterval);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Clears a failed release of the segment, see {@link #onSegmentReleased(Segment, TrackingToken)}.
     */
    @Override
    public void onSegmentClaimed(Segment segment,
                                 @Nullable TrackingToken from,
                                 CheckpointTrigger trigger) {
        failedReleases.remove(segment.getSegmentId());
        segmentIdToTrigger.put(segment.getSegmentId(), trigger);
    }

    /**
     * {@inheritDoc}
     * <p>
     * As {@link #onCheckpointAdvanced(Segment, TrackingToken, ProcessingContext)}, without a context to write in.
     */
    @Override
    public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment,
                                                                 TrackingToken requested) {
        return advance(segment, requested, null);
    }

    /**
     * {@inheritDoc}
     * <p>
     * First {@link CheckpointLatchCoordinator#flushCheckpoint(Segment, ProcessingContext) flushes} the coordinator,
     * right away on the calling thread, so it can write within the transaction of the given {@code context}. Fails as
     * soon as that flush fails; otherwise completes once the workflow work of the segment is safe as well.
     */
    @Override
    public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment,
                                                                 TrackingToken requested,
                                                                 ProcessingContext context) {
        return advance(segment, requested, context);
    }

    private CompletableFuture<TrackingToken> advance(Segment segment,
                                                     TrackingToken requested,
                                                     @Nullable ProcessingContext context) {
        return flush(() -> checkpointLatchCoordinator.flushCheckpoint(segment, context))
                .thenCompose(flushed -> awaitSafe(segment, requested));
    }

    private CompletableFuture<TrackingToken> awaitSafe(Segment segment, TrackingToken requested) {
        CompletableFuture<TrackingToken> result = new CompletableFuture<>();
        completeWhenSafe(segment, requested, result);
        return result;
    }

    private static CompletableFuture<Void> flush(Supplier<CompletableFuture<Void>> flush) {
        try {
            return flush.get();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Completes the given {@code result} with the {@code requested} token once the given {@code segment} holds no
     * unsafe checkpoint work, re-checking each time a latch is crossed.
     * <p>
     * Every re-check completes the same {@code result}, so no chain of futures builds up while the work stays unsafe
     * for many rounds. The coordinator attaches one latch to every unsafe execution, but only its first crossing
     * re-checks: later crossings of that latch are no-ops, so each crossing schedules at most one follow-up latch
     * instead of one per execution that shared the previous latch.
     * <p>
     * A failure to check or to attach a latch completes the {@code result} exceptionally. Re-checks run inside a latch
     * callback on a workflow thread, where a thrown exception would leave the {@code result} incomplete and skip the
     * other callbacks waiting on that execution.
     *
     * @param segment   the segment whose checkpoint is being advanced
     * @param requested the token to complete the {@code result} with
     * @param result    the future to complete once the checkpoint work of the {@code segment} is safe
     */
    private void completeWhenSafe(Segment segment,
                                  TrackingToken requested,
                                  CompletableFuture<TrackingToken> result) {
        try {
            if (!checkpointLatchCoordinator.hasUnsafeCheckpointWork(segment)) {
                result.complete(requested);
                return;
            }

            AtomicBoolean crossed = new AtomicBoolean();
            checkpointLatchCoordinator.addCheckpointLatch(segment, () -> {
                if (crossed.compareAndSet(false, true) && !result.isDone()) {
                    completeWhenSafe(segment, requested, result);
                }
            });
        } catch (RuntimeException e) {
            result.completeExceptionally(e);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * A release fails while the {@link CheckpointLatchCoordinator} holds the checkpoint of the segment. It then keeps
     * failing for every later report of the same release, until the segment is claimed again: the processor may report
     * a release more than once, and by the next report the engine may have dropped the work that held the checkpoint,
     * which was never made durable.
     * <p>
     * Otherwise, the release reports {@code requested} once the workflow work of the segment is safe and the
     * {@link CheckpointLatchCoordinator#flushRelease(Segment) release flush} of the coordinator completed. It fails
     * when that flush fails, which releases the segment without advancing its stored token.
     */
    @Override
    public CompletableFuture<TrackingToken> onSegmentReleased(Segment segment,
                                                              TrackingToken requested) {
        int segmentId = segment.getSegmentId();
        CompletableFuture<TrackingToken> released;
        if (failedReleases.contains(segmentId) || checkpointLatchCoordinator.holdsCheckpoint(segment)) {
            failedReleases.add(segmentId);
            released = CompletableFuture.failedFuture(new IllegalStateException(
                    "Segment " + segmentId + " holds its checkpoint: it has workflow work that is not durable yet, or "
                            + "its workflows could not be restored"));
        } else {
            var flushed = flush(() -> checkpointLatchCoordinator.flushRelease(segment));
            released = awaitSafe(segment, requested).thenCombine(flushed, (safe, ignored) -> safe);
        }
        return released
                // Only this segment's trigger dies with its claim; the segments still held keep checkpointing.
                .whenComplete((ignored, cause) -> {
                    segmentIdToTrigger.remove(segment.getSegmentId());
                    lastForwardNanos.remove(segment.getSegmentId());
                });
    }

    /**
     * Get and sets the {@link CheckpointTrigger} from the given {@code context}.
     * <p>
     * A batch context carries both the segment being handled and that segment's {@link CheckpointTrigger}, so this
     * registers the trigger under its own segment even when the claim callback was missed.
     *
     * @param context the current processor context
     */
    void getAndSetTriggerFrom(ProcessingContext context) {
        Segment.fromContext(context).ifPresent(
                segment -> CheckpointTrigger.fromContext(context)
                                            .ifPresent(trigger -> segmentIdToTrigger.put(segment.getSegmentId(),
                                                                                         trigger))
        );
    }

    /**
     * Requests a checkpoint of the given {@code segment} to be made at the given {@code token}.
     * <p>
     * The request is dropped when this node does not currently hold the segment, which includes a request without a
     * segment to attribute it to. The trigger of a segment that is not claimed is inert and ignores requests anyway;
     * pushing the token through any other segment's trigger would advance that segment past events it never handled. A
     * request can also arrive for a segment this node holds whose trigger has not been registered yet, and is dropped
     * the same way. That is safe: the stored token merely stays behind, so the events are re-processed after a restart
     * instead of being skipped.
     *
     * @param segment the segment the requested position belongs to, ignored when {@code null}
     * @param token   the token to request, ignored when {@code null}
     */
    void requestCheckpoint(@Nullable Segment segment, @Nullable TrackingToken token) {
        if (segment == null || token == null || checkpointLatchCoordinator.holdsCheckpoint(segment)) {
            return;
        }
        var trigger = segmentIdToTrigger.get(segment.getSegmentId());
        if (trigger != null && tryClaimForwardSlot(segment)) {
            trigger.requestCheckpoint(token);
        }
    }

    /**
     * Claims the right to forward a checkpoint request for the given {@code segment}, recording the time of the claim.
     * The map updates are compare-and-set, so of concurrent requests within one interval exactly one claims the slot.
     */
    private boolean tryClaimForwardSlot(Segment segment) {
        if (checkpointIntervalNanos == 0) {
            return true;
        }
        int segmentId = segment.getSegmentId();
        long now = System.nanoTime();
        Long last = lastForwardNanos.putIfAbsent(segmentId, now);
        if (last == null) {
            return true;
        }
        return now - last >= checkpointIntervalNanos && lastForwardNanos.replace(segmentId, last, now);
    }

    /**
     * Coordinates workflow work that must complete before checkpoint advancement.
     */
    @Internal
    public interface CheckpointLatchCoordinator {

        /**
         * Returns whether any {@link WorkflowExecution WorkflowExecutions} owned by the given {@code segment} still
         * makes checkpoint advancement unsafe.
         *
         * @param segment the segment whose checkpoint is being advanced
         * @return {@code true} when checkpoint advancement must wait, {@code false} otherwise
         */
        boolean hasUnsafeCheckpointWork(Segment segment);

        /**
         * Returns whether the stored position of the given {@code segment} must not advance at all: neither through a
         * checkpoint request, which is dropped, nor on its release, which fails.
         *
         * @param segment the segment whose checkpoint is being advanced
         * @return {@code true} when the stored position of the segment must stay where it is
         */
        boolean holdsCheckpoint(Segment segment);

        /**
         * Adds a checkpoint latch across the current set of {@link WorkflowExecution WorkflowExecutions} owned by the
         * given {@code segment} that are still performing tasks for the supported {@link WorkflowEngine}.
         * <p>
         * The given {@code latch} should be attached to all {@code WorkflowExecutions} that still have tasks to
         * perform. Or in other terms, executions that are "unsafe" to checkpoint on
         *
         * @param segment the segment whose checkpoint is being advanced
         * @param latch   the latch to invoke after all unsafe {@link WorkflowExecution WorkflowExecutions} have reached
         *                it
         */
        void addCheckpointLatch(Segment segment, Runnable latch);

        /**
         * Makes the state the given {@code segment} derived from the events it handled so far durable, ahead of a
         * checkpoint of the segment, within the given {@code context} where possible.
         * <p>
         * Defaults to a completed future: no other state needs to be durable before the checkpoint.
         *
         * @param segment the segment whose checkpoint is being advanced
         * @param context the context the processor stores the checkpoint in, or {@code null} when unknown
         * @return a future completing once the state is durable, or written within the {@code context}, or failing
         * when it could not be made durable, which fails the checkpoint
         */
        default CompletableFuture<Void> flushCheckpoint(Segment segment, @Nullable ProcessingContext context) {
            return CompletableFuture.completedFuture(null);
        }

        /**
         * Makes the state the given {@code segment} derived from the events it handled durable, ahead of its release.
         * <p>
         * Defaults to a completed future: no other state needs to be durable before the release.
         *
         * @param segment the segment being released
         * @return a future completing once the state is durable, or failing when it could not be made durable, which
         * releases the segment without advancing its stored token
         */
        default CompletableFuture<Void> flushRelease(Segment segment) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
