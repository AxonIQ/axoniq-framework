/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.workflow.runtime.execution;

import io.axoniq.framework.messaging.eventstreaming.checkpoint.CheckpointTrigger;
import io.axoniq.framework.messaging.eventstreaming.checkpoint.Checkpointing;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * A {@link Checkpointing} implementation dedicated for a {@link WorkflowEngine}, to separate check pointing behavior
 * from it.
 * <p>
 * This class has one job: coordinate checkpoint requests and complete
 * {@link #onCheckpointAdvanced(Segment, TrackingToken)} only after the host confirms that workflow-owned asynchronous
 * work is safe. It does not track
 * processor progress or decide when the engine should switch to live mode.
 * <p>
 * Everything here is kept <em>per segment</em>. The processor hands out one {@link CheckpointTrigger} per claimed
 * segment, a request through it advances only that segment's stored token, and it is inert once that claim ends.
 * <p>
 * The trigger map is concurrent, and its updates are deliberately small: they never invoke processor or workflow
 * callbacks while a claim is being recorded or dropped. The coordinator callback supplied to
 * {@link #onCheckpointAdvanced(Segment, TrackingToken)} re-enters the support only after all the
 * {@link WorkflowExecution} latches (attached by the {@link CheckpointLatchCoordinator}) have been crossed.
 *
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 0.2.0
 */
@Internal
public class WorkflowEngineCheckpointingSupport implements Checkpointing {

    private final CheckpointLatchCoordinator checkpointLatchCoordinator;
    private final Map<Integer, CheckpointTrigger> segmentIdToTrigger = new ConcurrentHashMap<>();

    /**
     * Creates checkpointing support for a {@link WorkflowEngine} using the given {@code checkpointLatchCoordinator}.
     *
     * @param checkpointLatchCoordinator adds a latch to coordinated {@link WorkflowExecution WorkflowExecutions} to
     *                                   ensure all have reached a safe point to advance the checkpoint
     */
    public WorkflowEngineCheckpointingSupport(CheckpointLatchCoordinator checkpointLatchCoordinator) {
        this.checkpointLatchCoordinator = requireNonNull(
                checkpointLatchCoordinator, "The CheckpointLatchCoordinator must not be null."
        );
    }

    @Override
    public void onSegmentClaimed(Segment segment,
                                 @Nullable TrackingToken from,
                                 CheckpointTrigger trigger) {
        segmentIdToTrigger.put(segment.getSegmentId(), trigger);
    }

    @Override
    public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment,
                                                                 TrackingToken requested) {
        CompletableFuture<TrackingToken> result = new CompletableFuture<>();
        if (!checkpointLatchCoordinator.hasUnsafeCheckpointWork(segment)) {
            result.complete(requested);
            return result;
        }

        checkpointLatchCoordinator.addCheckpointLatch(segment, () -> {
            if (result.isDone()) {
                return;
            }
            onCheckpointAdvanced(segment, requested).whenComplete((token, cause) -> {
                if (cause != null) {
                    result.completeExceptionally(cause);
                } else {
                    result.complete(token);
                }
            });
        });
        return result;
    }

    @Override
    public CompletableFuture<TrackingToken> onSegmentReleased(Segment segment,
                                                              TrackingToken requested) {
        return onCheckpointAdvanced(segment, requested)
                // Only this segment's trigger dies with its claim; the segments still held keep checkpointing.
                .whenComplete((ignored, cause) -> segmentIdToTrigger.remove(segment.getSegmentId()));
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
                                            .ifPresent(trigger -> onSegmentClaimed(segment, null, trigger))
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
        if (segment == null || token == null) {
            return;
        }
        var trigger = segmentIdToTrigger.get(segment.getSegmentId());
        if (trigger != null) {
            trigger.requestCheckpoint(token);
        }
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
    }
}
