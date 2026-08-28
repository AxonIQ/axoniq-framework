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

import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.WrappedToken;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChangedHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 * Replay state is intentionally kept separate from checkpoint advancement state: {@link #segmentIdToToken} tracks where
 * each segment's processor callback currently is, {@link #startupLatestToken} remembers the stream position that marked
 * the end of replay at startup, and {@link #liveSegments} gates whether a segment's executions should start
 * immediately. None of those concerns are required to ask for a checkpoint, so they live here instead of inside
 * checkpoint support.
 * <p>
 * Both the position and the live-mode flag are kept <em>per segment</em>. Segments of one processor catch up
 * independently, so each segment starts its workflow bodies exactly when it has caught up, and each restored execution
 * resumes from the position its own segment reached.
 * <p>
 * Replay state is read frequently from processor callbacks, so the {@code TrackingToken} references are
 * {@code volatile} and the per-segment state lives in concurrent collections. The engine-wide live-mode flag is an
 * {@link AtomicBoolean} and a segment's transition is the {@link Set#add(Object)} on {@link #liveSegments}, so each
 * transition happens once without a monitor. The live-mode callback runs after the successful transition, outside any
 * coordination mechanism.
 *
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 1.0.0
 */
@Internal
public class WorkflowEngineReplaySupport implements ReplayStatusChangedHandler {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngineReplaySupport.class);

    private final LiveModeActivatedCallback liveModeCallback;

    private final Set<Integer> liveSegments = ConcurrentHashMap.newKeySet();
    private final Map<Integer, TrackingToken> segmentIdToToken = new ConcurrentHashMap<>();
    @Nullable
    private volatile TrackingToken currentToken;
    @Nullable
    private volatile TrackingToken startupProcessorToken;
    @Nullable
    private volatile TrackingToken startupLatestToken;
    private final AtomicBoolean inLiveMode = new AtomicBoolean();

    /**
     * Creates replay support for the given live-mode activation callback.
     *
     * @param liveModeCallback the workflow-engine callback invoked when live mode starts
     */
    public WorkflowEngineReplaySupport(LiveModeActivatedCallback liveModeCallback) {
        this.liveModeCallback = requireNonNull(liveModeCallback, "The LiveModeActivatedCallback must not be null");
    }

    @Override
    public MessageStream.Empty<Message> handle(ReplayStatusChanged statusChange,
                                               ProcessingContext context) {
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
     * among unwrapped {@code TrackingTokens}. The normalized position is recorded against the segment handling the
     * callback, so each segment's progress is tracked on its own.
     *
     * @param context the current processing context
     * @return the normalized current {@link TrackingToken} of the handling segment, if present
     */
    @Nullable
    TrackingToken getAndSetTokenFrom(ProcessingContext context) {
        var segment = Segment.fromContext(context).orElse(null);
        TrackingToken.fromContext(context)
                     .ifPresent(token -> {
                         var normalized = WrappedToken.unwrapLowerBound(token);
                         currentToken = normalized;
                         if (segment != null && normalized != null) {
                             segmentIdToToken.put(segment.getSegmentId(), normalized);
                         }
                     });
        return currentToken(segment);
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
        startupProcessorToken = processorToken;
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
            startupProcessorToken = processorToken;
        }
    }

    /**
     * Switches to live mode if it has not happened yet.
     * <p>
     * When the given context carries a {@link Segment}, only that segment switches; a context without one is an
     * engine-wide switch, used by the startup path that decided no replay is pending at all.
     *
     * @return {@code true} if this call performed the transition, otherwise {@code false}
     */
    public boolean switchToLiveMode(ProcessingContext processingContext) {
        var segment = Segment.fromContext(processingContext).orElse(null);
        if (segment == null) {
            if (!inLiveMode.compareAndSet(false, true)) {
                return false;
            }
        } else if (!liveSegments.add(segment.getSegmentId())) {
            return false;
        }
        liveModeCallback.invoke(processingContext);
        return true;
    }

    /**
     * Returns whether the engine as a whole was declared live, independent of any segment.
     *
     * @return {@code true} once engine-wide live mode has been entered
     */
    public boolean inLiveMode() {
        return inLiveMode.get();
    }

    /**
     * Returns whether the given segment has finished replaying and may run workflow bodies.
     * <p>
     * The engine-wide flag answers only for non-segmented handling. A segment answers for itself alone: the engine-wide
     * flag is set by a startup that found nothing to replay, and a node can start that way while owning nothing at all.
     * Letting that flag stand in for a segment would declare every segment this node claims later live as well,
     * including one whose observed position is still behind the stream.
     *
     * @param segment the segment handling the current callback, or {@code null} when handling is not segmented
     * @return {@code true} once this segment has caught up
     */
    public boolean inLiveMode(@Nullable Segment segment) {
        return segment == null ? inLiveMode.get() : liveSegments.contains(segment.getSegmentId());
    }

    /**
     * Returns whether this engine has observed the given segment fall behind the stream position replay has to reach.
     * <p>
     * The decision rests on a position this engine saw a delivery at, not on the position the segment is claimed from.
     * Only a delivery on the segment itself can lift the deferral, through
     * {@link #validateIfReplayFinished(TrackingToken, ProcessingContext)}, so deferring a segment no delivery has ever
     * reached would park its instances for good: events destined for other segments advance its stored token without
     * ever calling its handler. A segment whose position this engine has not observed is therefore treated as caught
     * up, and {@code claimedFrom} seeds the restore position rather than this decision.
     *
     * @param segment     the segment being claimed
     * @param claimedFrom the stored position the segment resumes from, or {@code null} when it has consumed nothing
     * @return {@code true} when an observed position of this segment is behind the startup latest token
     */
    public boolean isReplaying(Segment segment, @Nullable TrackingToken claimedFrom) {
        var latest = startupLatestToken;
        var observed = segmentIdToToken.get(segment.getSegmentId());
        return latest != null && observed != null && !inLiveMode(segment) && !covers(observed, latest);
    }

    /**
     * Returns the most recently observed tracking token of the given segment.
     *
     * @param segment the segment whose position is asked for, or {@code null} when handling is not segmented
     * @return the current normalized tracking token, if present
     */
    @Nullable
    public TrackingToken currentToken(@Nullable Segment segment) {
        if (segment == null) {
            return currentToken;
        }
        var token = segmentIdToToken.get(segment.getSegmentId());
        // Falls back to the startup position, a lower bound of every segment, never to another segment's position.
        return token != null ? token : startupProcessorToken;
    }

    /**
     * Makes the processor token of the given segment visible in a context used to construct restored workflow
     * executions.
     * <p>
     * {@code claimedFrom} is the position the segment resumes at and is preferred when given: it is the segment's own
     * stored position, where the fallback is the position this engine observed or started at.
     *
     * @param segment           the segment whose executions are restored, or {@code null} during engine startup
     * @param claimedFrom       the position the segment resumes from, or {@code null} when it is not known
     * @param processingContext the context used to create restored executions
     */
    public void initializeRestoreProcessingContext(@Nullable Segment segment,
                                                   @Nullable TrackingToken claimedFrom,
                                                   ProcessingContext processingContext) {
        var processorToken = claimedFrom != null ? claimedFrom : currentToken(segment);
        if (processorToken != null && !processingContext.resources().containsKey(TrackingToken.RESOURCE_KEY)) {
            processingContext.putResource(TrackingToken.RESOURCE_KEY, processorToken);
        }
    }

    /**
     * Validates if the replay has finished for the given {@code token}, to be invoked by
     * {@link WorkflowEngine#handle(EventMessage, ProcessingContext)} upon processing any event.
     * <p>
     * The decision is per segment: a segment reaching {@link #startupLatestToken} switches only itself, leaving the
     * segments still catching up in replay mode.
     *
     * @param token   the token of the {@link EventMessage} that's just been handled by
     *                {@link WorkflowEngine#handle(EventMessage, ProcessingContext)}
     * @param context the context used to {@link #switchToLiveMode(ProcessingContext) switch to live mode with}
     */
    void validateIfReplayFinished(@Nullable TrackingToken token,
                                  ProcessingContext context) {
        var latest = startupLatestToken;
        if (!inLiveMode(Segment.fromContext(context).orElse(null))
                && (latest == null || token != null && token.covers(latest))) {
            switchToLiveMode(context);
        }
    }

    private static boolean covers(@Nullable TrackingToken token, TrackingToken other) {
        return token != null && token.covers(other);
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
        void invoke(ProcessingContext context);
    }
}
