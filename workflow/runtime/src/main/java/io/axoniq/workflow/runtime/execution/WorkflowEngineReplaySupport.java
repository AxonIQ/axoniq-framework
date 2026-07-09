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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.WrappedToken;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChangedHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Support object that owns replay tracking and the transition from replay mode to live mode for one workflow engine.
 * <p>
 * The workflow engine itself only needs a small replay API:
 * capture the current tracking token from the processor callback, seed restored execution contexts with the processor
 * position, initialize startup replay boundaries, and switch to live mode exactly once when replay has completed.
 * This class owns that lifecycle so the engine no longer mixes replay bookkeeping with event routing or checkpoint
 * advancement.
 * <p>
 * Replay state is intentionally kept separate from checkpoint advancement state:
 * {@link #currentTrackingToken} tracks where the processor callback currently is,
 * {@link #startupLatestToken} remembers the stream position that marked the end of replay at startup, and
 * {@link #liveMode} gates whether restored executions should start immediately. None of those concerns are required to
 * ask for a checkpoint, so they live here instead of inside checkpoint support.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class WorkflowEngineReplaySupport implements ReplayStatusChangedHandler {

    /**
     * Callback invoked once replay has transitioned to live mode.
     */
    @FunctionalInterface
    public interface Host {

        /**
         * Reacts to the first transition from replay mode to live mode.
         */
        void onLiveModeActivated();
    }

    private final static Logger logger = LoggerFactory.getLogger(WorkflowEngineReplaySupport.class);
    private final Host host;
    private volatile boolean liveMode;
    @Nullable
    private volatile TrackingToken currentTrackingToken;
    @Nullable
    private volatile TrackingToken startupLatestToken;

    /**
     * Creates replay support for the given host.
     *
     * @param host the workflow-engine callback invoked when live mode starts
     */
    public WorkflowEngineReplaySupport(@Nonnull Host host) {
        this.host = Objects.requireNonNull(host, "Replay host must not be null");
    }

    /**
     * Initializes replay tracking from the processor token known at startup.
     *
     * @param processorToken the token currently stored for the segment
     * @param latestToken the latest known token at startup, used to detect replay completion
     */
    public void initializeReplayTracking(@Nullable TrackingToken processorToken,
                                         @Nullable TrackingToken latestToken) {
        currentTrackingToken = processorToken;
        startupLatestToken = latestToken;
    }

    /**
     * Switches to live mode if it has not happened yet.
     *
     * @return {@code true} if this call performed the transition, otherwise {@code false}
     */
    public boolean switchToLiveMode() {
        synchronized (this) {
            if (liveMode) {
                return false;
            }
            liveMode = true;
        }
        host.onLiveModeActivated();
        return true;
    }

    /**
     * Returns whether replay has already finished and live mode is active.
     *
     * @return {@code true} once live mode has been entered
     */
    public boolean isLiveMode() {
        return liveMode;
    }

    /**
     * Returns the most recently observed tracking token.
     *
     * @return the current normalized tracking token, if present
     */
    @Nullable
    public TrackingToken currentTrackingToken() {
        return currentTrackingToken;
    }

    /**
     * Observes replay-related state from the current processor callback.
     * <p>
     * If a tracking token is present in the context, it is normalized to the lower bound so replay tokens are handled
     * consistently with ordinary tracking tokens.
     *
     * @param processingContext the current processor context
     * @return the normalized current tracking token, if present
     */
    @Nullable
    TrackingToken observeProcessingContext(@Nonnull ProcessingContext processingContext) {
        var token = (TrackingToken) processingContext.resources().get(TrackingToken.RESOURCE_KEY);
        if (token != null) {
            currentTrackingToken = WrappedToken.unwrapLowerBound(token);
        }
        return currentTrackingToken;
    }

    /**
     * Makes the processor token visible in a context used to construct restored workflow executions.
     *
     * @param processingContext the context used to create restored executions
     */
    public void initializeRestoreProcessingContext(@Nonnull ProcessingContext processingContext) {
        var processorToken = currentTrackingToken;
        if (processorToken != null && !processingContext.resources().containsKey(TrackingToken.RESOURCE_KEY)) {
            processingContext.putResource(TrackingToken.RESOURCE_KEY, processorToken);
        }
    }

    /**
     * Advances replay progress for the currently handled callback and switches to live mode once replay has caught up.
     *
     * @param currentToken the normalized token associated with the observed callback
     */
    void advanceReplayPosition(@Nullable TrackingToken currentToken) {
        var latest = startupLatestToken;
        if (latest != null && !liveMode && covers(currentToken, latest)) {
            switchToLiveMode();
        }
    }

    /**
     * Reacts to Axon's replay-status callbacks.
     *
     * @param statusChange the replay-status change notification
     * @param context the current processor context
     * @return an empty stream because replay-status callbacks never emit follow-up messages
     */
    @Nonnull
    @Override
    public MessageStream.Empty<Message> handle(@Nonnull ReplayStatusChanged statusChange,
                                               @Nonnull ProcessingContext context) {
        observeProcessingContext(context);
        logger.debug("Replay status changed to {} at {}",
                     statusChange.status(),
                     context.resources().get(TrackingToken.RESOURCE_KEY));
        if (!statusChange.status().isReplay() && !switchToLiveMode()) {
            logger.warn("Workflow execution is already started.");
        }
        return MessageStream.empty();
    }

    private static boolean covers(@Nullable TrackingToken current,
                                  @Nullable TrackingToken target) {
        if (target == null) {
            return true;
        }
        return current != null && (current.covers(target) || current.samePositionAs(target));
    }
}
