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
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.WrappedToken;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChangedHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * Support object that owns replay tracking and the transition from replay mode to live mode for a
 * {@link WorkflowEngine} by implementing the {@link ReplayStatusChangedHandler}.
 * <p>
 * The {@code WorkflowEngine} itself only needs a small replay API: capture the current {@link TrackingToken} from the
 * processor callback, seed restored execution contexts with the processor position, initialize startup replay
 * boundaries, and switch to live mode exactly once when replay has completed. This class owns that lifecycle so the
 * engine no longer mixes replay bookkeeping with
 * {@link org.axonframework.messaging.eventhandling.EventHandler event handling} or
 * {@link io.axoniq.framework.messaging.eventstreaming.checkpoint.Checkpointing checkpoint advancement}.
 * <p>
 * Replay state is intentionally kept separate from checkpoint advancement state: {@link #currentToken} tracks where the
 * processor callback currently is, {@link #startupLatestToken} remembers the stream position that marked the end of
 * replay at startup, and {@link #inLiveMode} gates whether restored executions should start immediately. None of those
 * concerns are required to ask for a checkpoint, so they live here instead of inside checkpoint support.
 * <p>
 * Replay state is read frequently from processor callbacks, so the {@code TrackingToken} references are
 * {@code volatile}. The live-mode state uses an {@link AtomicBoolean} to perform the one-time transition atomically
 * without a monitor. The live-mode callback runs after the successful transition, outside any coordination mechanism.
 *
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 1.0.0
 */
@Internal
public class WorkflowEngineReplaySupport implements ReplayStatusChangedHandler {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngineReplaySupport.class);

    private final LiveModeActivatedCallback liveModeCallback;

    @Nullable
    private volatile TrackingToken currentToken;
    @Nullable
    private volatile TrackingToken startupLatestToken;
    private final AtomicBoolean inLiveMode = new AtomicBoolean();

    /**
     * Creates replay support for the given live-mode activation callback.
     *
     * @param liveModeCallback the workflow-engine callback invoked when live mode starts
     */
    public WorkflowEngineReplaySupport(@Nonnull LiveModeActivatedCallback liveModeCallback) {
        this.liveModeCallback = requireNonNull(liveModeCallback, "The LiveModeActivatedCallback must not be null");
    }

    @Nonnull
    @Override
    public MessageStream.Empty<Message> handle(@Nonnull ReplayStatusChanged statusChange,
                                               @Nonnull ProcessingContext context) {
        getAndSetTokenFrom(context);
        logger.debug("Replay status changed from [{}] to [{}].",
                     statusChange.status(), context.resources().get(TrackingToken.RESOURCE_KEY));
        if (!statusChange.status().isReplay() && !switchToLiveMode(context)) {
            logger.warn("Workflow execution is already started.");
        }
        return MessageStream.empty();
    }

    /**
     * Get and sets the {@link TrackingToken} from the given {@code context} when available.
     * <p>
     * If a {@code TrackingToken} is present in the context, it is normalized to the lower bound. This ensures
     * {@link WrappedToken} like the
     * {@link org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken} are handled consistently
     * among unwrapped {@code TrackingTokens}.
     *
     * @param context the current processing context
     * @return the normalized current {@link TrackingToken}, if present
     */
    @Nullable
    TrackingToken getAndSetTokenFrom(@Nonnull ProcessingContext context) {
        TrackingToken.fromContext(context)
                     .ifPresent(token -> currentToken = WrappedToken.unwrapLowerBound(token));
        return currentToken;
    }

    /**
     * Sets the {@link TrackingToken TrackingTokens} present when initializing a {@link WorkflowEngine}.
     *
     * @param processorToken the token currently stored by the
     *                       {@link
     *                       org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor}
     *                       backing the {@link WorkflowEngine}
     * @param latestToken    the latest known token at startup, used to detect replay completion
     */
    public void setInitialEngineTokens(@Nullable TrackingToken processorToken,
                                       @Nullable TrackingToken latestToken) {
        currentToken = processorToken;
        startupLatestToken = latestToken;
    }

    /**
     * Seeds the current processor token when replay tracking has not been initialized yet.
     *
     * @param processorToken processor token supplied during engine startup
     */
    void setCurrentTokenIfNull(@Nullable TrackingToken processorToken) {
        if (currentToken == null) {
            currentToken = processorToken;
        }
    }

    /**
     * Switches to live mode if it has not happened yet.
     *
     * @return {@code true} if this call performed the transition, otherwise {@code false}
     */
    public boolean switchToLiveMode(@Nonnull ProcessingContext processingContext) {
        if (!inLiveMode.compareAndSet(false, true)) {
            return false;
        }
        liveModeCallback.invoke(processingContext);
        return true;
    }

    /**
     * Returns whether replay has already finished and live mode is active.
     *
     * @return {@code true} once live mode has been entered
     */
    public boolean inLiveMode() {
        return inLiveMode.get();
    }

    /**
     * Returns the most recently observed tracking token.
     *
     * @return the current normalized tracking token, if present
     */
    @Nullable
    public TrackingToken currentToken() {
        return currentToken;
    }

    /**
     * Validates if the replay has finished for the given {@code token}, to be invoked by
     * {@link WorkflowEngine#handle(EventMessage, ProcessingContext)} upon processing any event.
     *
     * @param token   the token of the {@link EventMessage} that's just been handled by
     *                {@link WorkflowEngine#handle(EventMessage, ProcessingContext)}
     * @param context the context used to {@link #switchToLiveMode(ProcessingContext) switch to live mode with}
     */
    void validateIfReplayFinished(@Nullable TrackingToken token,
                                  @Nonnull ProcessingContext context) {
        var latest = startupLatestToken;
        if (!inLiveMode.get() && (latest == null || token != null && token.covers(latest))) {
            switchToLiveMode(context);
        }
    }

    /**
     * Callback invoked once event processing has transitioned to live mode.
     */
    @Internal
    @FunctionalInterface
    public interface LiveModeActivatedCallback {

        /**
         * Reacts to the first transition from replay mode to live mode.
         *
         * @param context the current processor context
         */
        void invoke(@Nonnull ProcessingContext context);
    }
}
