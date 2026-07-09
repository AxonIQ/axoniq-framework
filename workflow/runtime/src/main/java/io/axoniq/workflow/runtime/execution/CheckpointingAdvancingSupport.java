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
import org.axonframework.messaging.eventhandling.processing.streaming.token.WrappedToken;

import java.util.concurrent.CompletableFuture;

/**
 * Base support for event handlers that advance streaming checkpoints only after workflow-owned asynchronous work has
 * become safe.
 * <p>
 * This class hides the stateful checkpoint orchestration that would otherwise clutter {@link WorkflowEngine}:
 * <ul>
 *     <li>capturing and normalizing the current tracking token from the processing context,</li>
 *     <li>remembering the latest replay target so live mode can start as soon as replay has caught up,</li>
 *     <li>holding the current {@link CheckpointTrigger} for the claimed segment, and</li>
 *     <li>coalescing checkpoint requests until a trigger is available while still allowing immediate forwarding once
 *     the processor owns a trigger.</li>
 * </ul>
 * <p>
 * The subclass only implements the workflow-specific parts:
 * <ul>
 *     <li>detect whether any owned execution still has unsafe work,</li>
 *     <li>schedule a checkpoint barrier across those executions, and</li>
 *     <li>start live-mode execution when replay has truly finished.</li>
 * </ul>
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
abstract class CheckpointingAdvancingSupport implements Checkpointing {

    private volatile boolean liveMode;
    @Nullable
    private volatile TrackingToken currentTrackingToken;
    @Nullable
    private volatile TrackingToken startupLatestToken;
    @Nullable
    private CheckpointTrigger checkpointTrigger;
    @Nullable
    private TrackingToken pendingCheckpointToken;

    /**
     * Captures checkpoint-related context for the current processor callback.
     * <p>
     * This method normalizes the current tracking token and registers a {@link CheckpointTrigger} if the processing
     * context exposes one.
     *
     * @param processingContext the current processor context
     * @return the normalized current tracking token, if present
     */
    @Nullable
    protected final TrackingToken observeCheckpointContext(@Nonnull ProcessingContext processingContext) {
        var token = (TrackingToken) processingContext.resources().get(TrackingToken.RESOURCE_KEY);
        if (token != null) {
            currentTrackingToken = WrappedToken.unwrapLowerBound(token);
        }
        CheckpointTrigger.fromContext(processingContext).ifPresent(this::onSegmentClaimed);
        return currentTrackingToken;
    }

    /**
     * Advances checkpoint orchestration for the observed tracking token.
     * <p>
     * The token is requested as a processor checkpoint and may trigger the transition to live mode when replay has
     * caught up.
     *
     * @param currentToken the normalized token associated with the observed callback
     */
    protected final void advanceCheckpointing(@Nullable TrackingToken currentToken) {
        requestCheckpoint(currentToken);
        switchToLiveModeIfCaughtUp(currentToken);
    }

    /**
     * Makes the processor token visible in a context used to rehydrate workflow executions.
     * <p>
     * Restored executions run before replay catch-up resumes. Seeding the processing context with the processor token
     * preserves the segment position in contexts that were created outside the live event-handling callback.
     *
     * @param processingContext the context used to construct restored executions
     */
    protected final void initializeRestoreProcessingContext(@Nonnull ProcessingContext processingContext) {
        var processorToken = currentTrackingToken;
        if (processorToken != null && !processingContext.resources().containsKey(TrackingToken.RESOURCE_KEY)) {
            processingContext.putResource(TrackingToken.RESOURCE_KEY, processorToken);
        }
    }

    /**
     * Initializes checkpoint state from the processor's current position.
     *
     * @param processorToken the processor token currently stored for the segment
     * @param latestToken the latest known token at startup, used to detect replay completion
     */
    public final void initializeCheckpointing(@Nullable TrackingToken processorToken,
                                              @Nullable TrackingToken latestToken) {
        currentTrackingToken = processorToken;
        startupLatestToken = latestToken;
    }

    /**
     * Returns whether the processor has already switched to live mode.
     *
     * @return {@code true} once live mode has been entered
     */
    protected final boolean isLiveMode() {
        return liveMode;
    }

    /**
     * Returns the most recently observed tracking token.
     *
     * @return the current normalized tracking token, if present
     */
    @Nullable
    protected final TrackingToken currentTrackingToken() {
        return currentTrackingToken;
    }

    /**
     * Forces the transition to live mode if it has not happened yet.
     *
     * @return {@code true} if this call performed the transition, otherwise {@code false}
     */
    protected final boolean activateLiveMode() {
        synchronized (this) {
            if (liveMode) {
                return false;
            }
            liveMode = true;
        }
        onLiveModeActivated();
        return true;
    }

    /**
     * Requests a processor checkpoint for the given token.
     * <p>
     * Until a {@link CheckpointTrigger} is available, requests are coalesced to their upper bound. Once the trigger is
     * present, requests are forwarded immediately.
     *
     * @param token the token to request, ignored when {@code null}
     */
    final synchronized void requestCheckpoint(@Nullable TrackingToken token) {
        if (token == null) {
            return;
        }
        pendingCheckpointToken = upperBound(pendingCheckpointToken, token);
        flushPendingCheckpointRequest();
    }

    @Nonnull
    @Override
    public final CompletableFuture<TrackingToken> onCheckpointAdvanced(@Nonnull Segment segment,
                                                                       @Nonnull TrackingToken requested) {
        if (hasUnsafeCheckpointWork()) {
            var result = new CompletableFuture<TrackingToken>();
            var scheduled = scheduleCheckpointIntent(() -> {
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
            if (!scheduled) {
                if (!hasUnsafeCheckpointWork()) {
                    return CompletableFuture.completedFuture(requested);
                }
                result.completeExceptionally(new IllegalStateException(
                        "Checkpoint requested while workflow work is unsafe, but no checkpoint intent could be scheduled."
                ));
            }
            return result;
        }
        return CompletableFuture.completedFuture(requested);
    }

    @Override
    public final void onSegmentClaimed(@Nonnull Segment segment,
                                       @Nonnull CheckpointTrigger trigger) {
        onSegmentClaimed(trigger);
    }

    @Override
    public final CompletableFuture<TrackingToken> onSegmentReleased(@Nonnull Segment segment,
                                                                    @Nonnull TrackingToken requested) {
        return onCheckpointAdvanced(segment, requested)
                .whenComplete((ignored, cause) -> clearCheckpointTrigger());
    }

    /**
     * Returns whether any subclass-owned execution still makes checkpoint advancement unsafe.
     *
     * @return {@code true} when checkpoint advancement must wait
     */
    protected abstract boolean hasUnsafeCheckpointWork();

    /**
     * Schedules checkpoint barrier tasks across the subclass-owned executions.
     *
     * @param onDrained callback to invoke once the scheduled barriers have been crossed
     * @return {@code true} if at least one barrier was scheduled
     */
    protected abstract boolean scheduleCheckpointIntent(@Nonnull Runnable onDrained);

    /**
     * Invoked exactly once when replay has transitioned into live mode.
     */
    protected abstract void onLiveModeActivated();

    private void switchToLiveModeIfCaughtUp(@Nullable TrackingToken currentToken) {
        var latest = startupLatestToken;
        if (latest != null && !liveMode && covers(currentToken, latest)) {
            activateLiveMode();
        }
    }

    private synchronized void onSegmentClaimed(@Nonnull CheckpointTrigger trigger) {
        checkpointTrigger = trigger;
        flushPendingCheckpointRequest();
    }

    private synchronized void flushPendingCheckpointRequest() {
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

    private static boolean covers(@Nullable TrackingToken current,
                                  @Nullable TrackingToken target) {
        if (target == null) {
            return true;
        }
        return current != null && (current.covers(target) || current.samePositionAs(target));
    }

    private static TrackingToken upperBound(@Nullable TrackingToken current,
                                            @Nonnull TrackingToken candidate) {
        return current == null ? candidate : current.upperBound(candidate);
    }
}
