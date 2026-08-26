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
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * A {@link Checkpointing} implementation dedicated for a {@link WorkflowEngine}, to separate check pointing behavior
 * from it.
 * <p>
 * This class has one job: coordinate checkpoint requests and complete
 * {@link #onCheckpointAdvanced(Segment, TrackingToken)} only after the host confirms that workflow-owned asynchronous
 * work is safe. It does not track
 * {@link org.axonframework.messaging.eventhandling.replay.ReplayStatusChangedHandler replay progress or decide when the
 * engine should switch to live mode}.
 * <p>
 * The checkpoint trigger and pending checkpoint token are coordinated through this instance's monitor. Their updates
 * are deliberately small and never invoke processor or workflow callbacks while the monitor is held. The coordinator
 * callback supplied to {@link #onCheckpointAdvanced(Segment, TrackingToken)} runs outside that monitor and re-enters
 * the support only after all the {@link WorkflowExecution} latches (attached by the {@link CheckpointLatchCoordinator})
 * have been crossed.
 *
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 1.0.0
 */
@Internal
public class WorkflowEngineCheckpointingSupport implements Checkpointing {

    private final CheckpointLatchCoordinator checkpointLatchCoordinator;
    @Nullable
    private CheckpointTrigger checkpointTrigger;
    @Nullable
    private TrackingToken pendingCheckpointToken;

    /**
     * Creates checkpointing support for a {@link WorkflowEngine} using the given {@code checkpointLatchCoordinator}.
     *
     * @param checkpointLatchCoordinator adds a latch to coordinated {@link WorkflowExecution WorkflowExecutions} to
     *                                   ensure all have reached a safe point to advance the checkpoint
     */
    public WorkflowEngineCheckpointingSupport(@NonNull CheckpointLatchCoordinator checkpointLatchCoordinator) {
        this.checkpointLatchCoordinator = requireNonNull(
                checkpointLatchCoordinator, "The CheckpointLatchCoordinator must not be null."
        );
    }

    @Override
    public void onSegmentClaimed(@NonNull Segment segment,
                                 @Nullable TrackingToken from,
                                 @NonNull CheckpointTrigger trigger) {
        setTriggerAndFlush(trigger);
    }

    @NonNull
    @Override
    public CompletableFuture<TrackingToken> onCheckpointAdvanced(@NonNull Segment segment,
                                                                 @NonNull TrackingToken requested) {
        CompletableFuture<TrackingToken> result = new CompletableFuture<>();
        if (!checkpointLatchCoordinator.hasUnsafeCheckpointWork()) {
            result.complete(requested);
            return result;
        }

        checkpointLatchCoordinator.addCheckpointLatch(() -> {
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

    @NonNull
    @Override
    public CompletableFuture<TrackingToken> onSegmentReleased(@NonNull Segment segment,
                                                              @NonNull TrackingToken requested) {
        return onCheckpointAdvanced(segment, requested)
                .whenComplete((ignored, cause) -> clearCheckpointTrigger());
    }

    private synchronized void clearCheckpointTrigger() {
        checkpointTrigger = null;
    }

    /**
     * Get and sets the {@link CheckpointTrigger} from the given {@code context}.
     * <p>
     * If the {@code context} already holds a {@link CheckpointTrigger}, this method registers it so pending checkpoint
     * requests can be forwarded immediately.
     *
     * @param context the current processor context
     */
    void getAndSetTriggerFrom(@NonNull ProcessingContext context) {
        CheckpointTrigger.fromContext(context)
                         .ifPresent(this::setTriggerAndFlush);
    }

    private synchronized void setTriggerAndFlush(@NonNull CheckpointTrigger trigger) {
        checkpointTrigger = trigger;
        flushPendingCheckpointRequest();
    }

    /**
     * Requests a checkpoint to be made at the given {@code token}.
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
        pendingCheckpointToken = pendingCheckpointToken == null ? token : pendingCheckpointToken.upperBound(token);
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

    /**
     * Coordinates workflow work that must complete before checkpoint advancement.
     */
    @Internal
    public interface CheckpointLatchCoordinator {

        /**
         * Returns whether any owned {@link WorkflowExecution WorkflowExecutions} still makes checkpoint advancement
         * unsafe.
         *
         * @return {@code true} when checkpoint advancement must wait, {@code false} otherwise
         */
        boolean hasUnsafeCheckpointWork();

        /**
         * Adds a checkpoint latch across the current set of {@link WorkflowExecution WorkflowExecutions} that are still
         * performing tasks for the supported {@link WorkflowEngine}.
         * <p>
         * The given {@code latch} should be attached to all {@code WorkflowExecutions} that still have tasks to
         * perform. Or in other terms, executions that are "unsafe" to checkpoint on
         *
         * @param latch the latch to invoke after all unsafe {@link WorkflowExecution WorkflowExecutions} have reached
         *              it
         */
        void addCheckpointLatch(@NonNull Runnable latch);
    }
}
