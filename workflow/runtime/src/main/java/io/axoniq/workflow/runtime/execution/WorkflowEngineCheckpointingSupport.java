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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Support object that owns processor-checkpoint advancement for one workflow engine.
 * <p>
 * This class has one job: coordinate checkpoint requests and complete
 * {@link #onCheckpointAdvanced(Segment, TrackingToken)} only after the host confirms that workflow-owned asynchronous
 * work is safe. It does not track replay progress or decide when the engine should switch to live mode.
 * <p>
 * The checkpoint trigger and pending checkpoint token are coordinated through this instance's monitor. Their updates
 * are deliberately small and never invoke processor or workflow callbacks while the monitor is held. The coordinator
 * callback supplied to {@link #onCheckpointAdvanced(Segment, TrackingToken)} runs outside that monitor and re-enters
 * the support only after the workflow barriers have been crossed.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class WorkflowEngineCheckpointingSupport implements Checkpointing {

    /**
     * Coordinates workflow work that must complete before checkpoint advancement.
     */
    @Internal
    public interface CheckpointBarrierCoordinator {

        /**
         * Returns whether any owned workflow execution still makes checkpoint advancement unsafe.
         *
         * @return {@code true} when checkpoint advancement must wait
         */
        boolean hasUnsafeCheckpointWork();

        /**
         * Schedules checkpoint barrier tasks across the owned workflow executions.
         * <p>
         * Implementations must invoke {@code onDrained} even when no execution currently needs a barrier. The callback
         * re-checks checkpoint safety, covering work appended after the coordinator takes its execution snapshot.
         *
         * @param onDrained callback to invoke once the scheduled barriers have been crossed
         */
        void scheduleCheckpointIntent(@Nonnull Runnable onDrained);
    }

    private final CheckpointBarrierCoordinator checkpointBarrierCoordinator;
    @Nullable
    private CheckpointTrigger checkpointTrigger;
    @Nullable
    private TrackingToken pendingCheckpointToken;

    /**
     * Creates checkpointing support for the given checkpoint-barrier coordinator.
     *
     * @param checkpointBarrierCoordinator coordinates workflow work before checkpoint advancement
     */
    public WorkflowEngineCheckpointingSupport(@Nonnull CheckpointBarrierCoordinator checkpointBarrierCoordinator) {
        this.checkpointBarrierCoordinator = Objects.requireNonNull(
                checkpointBarrierCoordinator,
                "Checkpoint barrier coordinator must not be null"
        );
    }

    /**
     * Observes processor context associated with an event-handling callback.
     * <p>
     * If the processor already exposes a {@link CheckpointTrigger}, this method registers it so pending checkpoint
     * requests can be forwarded immediately.
     *
     * @param processingContext the current processor context
     */
    void observeProcessingContext(@Nonnull ProcessingContext processingContext) {
        CheckpointTrigger.fromContext(processingContext).ifPresent(this::onSegmentClaimed);
    }

    /**
     * Requests a processor checkpoint for the given token.
     * <p>
     * Until a {@link CheckpointTrigger} is available, requests are coalesced to their upper bound. Once the trigger is
     * present, requests are forwarded immediately.
     *
     * @param token the token to request, ignored when {@code null}
     */
    synchronized void requestCheckpoint(@Nullable TrackingToken token) {
        if (token == null) {
            return;
        }
        pendingCheckpointToken = upperBound(pendingCheckpointToken, token);
        flushPendingCheckpointRequest();
    }

    @Nonnull
    @Override
    public CompletableFuture<TrackingToken> onCheckpointAdvanced(@Nonnull Segment segment,
                                                                 @Nonnull TrackingToken requested) {
        if (checkpointBarrierCoordinator.hasUnsafeCheckpointWork()) {
            var result = new CompletableFuture<TrackingToken>();
            checkpointBarrierCoordinator.scheduleCheckpointIntent(() -> {
                if (result.isDone()) {
                    return;
                }
                onCheckpointAdvanced(segment, requested)
                        .whenComplete((token, cause) -> {
                            if (cause != null) {
                                result.completeExceptionally(cause);
                            } else {
                                result.complete(token);
                            }
                        });
            });
            return result;
        }
        return CompletableFuture.completedFuture(requested);
    }

    @Override
    public void onSegmentClaimed(@Nonnull Segment segment,
                                 @Nonnull CheckpointTrigger trigger) {
        onSegmentClaimed(trigger);
    }

    @Override
    public CompletableFuture<TrackingToken> onSegmentReleased(@Nonnull Segment segment,
                                                              @Nonnull TrackingToken requested) {
        return onCheckpointAdvanced(segment, requested)
                .whenComplete((ignored, cause) -> clearCheckpointTrigger());
    }

    private synchronized void onSegmentClaimed(@Nonnull CheckpointTrigger trigger) {
        checkpointTrigger = trigger;
        flushPendingCheckpointRequest();
    }

    private void flushPendingCheckpointRequest() {
        var trigger = checkpointTrigger;
        if (trigger == null) {
            return;
        }
        var requested = pendingCheckpointToken;
        if (requested == null) {
            return;
        }
        trigger.requestCheckpoint(requested);
        pendingCheckpointToken = null;
    }

    private synchronized void clearCheckpointTrigger() {
        checkpointTrigger = null;
    }

    private static TrackingToken upperBound(@Nullable TrackingToken current,
                                            @Nonnull TrackingToken candidate) {
        return current == null ? candidate : current.upperBound(candidate);
    }
}
