/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.execution.SegmentClaim.BodyStart;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The current {@link SegmentClaim claim} of each workflow-engine segment, and the activation of each claim once it
 * consumed the startup backlog, which starts its restored workflow executions.
 * <p>
 * A segment has at most one current claim. Starting a claim of a segment, or releasing the segment, ends the claim it
 * replaces, which waits for an action or activation of that claim in progress, so the previous claim never starts
 * executions of the next one.
 * <p>
 * A claim activates once its segment caught up with the startup head: right away, when there is no startup head, or
 * when the segment resumes from a position that covers it or has consumed nothing yet, or else once an event the
 * segment handles, or the position of the segment, covers it. The {@link Activation} the engine hands to
 * {@link #activateOrAwaitCatchUp(SegmentClaim, TrackingToken, Activation)} or
 * {@link #activateOnDelivery(Segment, TrackingToken, Activation)} then runs, exactly once per claim.
 * <p>
 * A segment that handles an event, or fails to, while it has no current claim gets an <em>implicit</em> claim: one
 * without an execution context, recorded on first use. That happens when the processor hands events of a segment to the
 * engine without reporting the claim of that segment through
 * {@link WorkflowEngine#restoreWorkflowsFor(Segment, TrackingToken, ProcessingContext, ProcessingContext)}. An implicit
 * claim restores nothing and its executions never hibernate, but it still activates, and still holds its checkpoint
 * when its segment fails to handle an event.
 *
 * @author Simon Zambrovski
 * @author Allard Buijze
 * @since 5.4.0
 */
@Internal
final class SegmentClaims {

    /**
     * Time between two reads of the position of a segment that waits to catch up with the startup head.
     */
    static final Duration CATCH_UP_POLL_INTERVAL = Duration.ofMillis(200);
    /**
     * Number of consecutive polls without a known position after which a segment waiting to catch up is no longer
     * polled: the processor no longer holds it, so its position will not become known again.
     */
    static final int CATCH_UP_POLLS_WITHOUT_POSITION = 10;
    private static final Logger logger = LoggerFactory.getLogger(SegmentClaims.class);

    private final Map<Integer, SegmentClaim> claims = new ConcurrentHashMap<>();
    @Nullable
    private volatile TrackingToken startupHead;
    private volatile Function<Segment, Optional<TrackingToken>> segmentPositions = segment -> Optional.empty();
    private volatile boolean stopped;
    // Runs each catch-up poll once the poll interval passed. Package-private so a test can run the polls itself,
    // instead of waiting for the interval to pass.
    Consumer<Runnable> catchUpPolls =
            poll -> CompletableFuture.delayedExecutor(CATCH_UP_POLL_INTERVAL.toMillis(), TimeUnit.MILLISECONDS)
                                     .execute(poll);

    /**
     * Records the position a claim must reach before it activates.
     *
     * @param startupHead the latest position observed while starting the processor, or {@code null} when every claim
     *                    may start the bodies of its workflows right away
     */
    void initialize(@Nullable TrackingToken startupHead) {
        this.startupHead = startupHead;
    }

    /**
     * Sets the source of the position each claimed segment has consumed up to, which a claim behind the startup head
     * polls to learn that it caught up.
     *
     * @param segmentPositions returns the position the given segment consumed up to, or empty when unknown
     */
    void setSegmentPositions(Function<Segment, Optional<TrackingToken>> segmentPositions) {
        this.segmentPositions = Objects.requireNonNull(segmentPositions, "The segment positions must not be null.");
    }

    /**
     * Stops polling the positions of the segments waiting to catch up, as the engine shuts down.
     */
    void stop() {
        stopped = true;
    }

    /**
     * Starts a new claim of the given {@code segment}, ending any claim of it that is still recorded. Returns once an
     * activation of that previous claim in progress has finished, so it cannot start executions of the new claim.
     *
     * @param segment          the claimed segment
     * @param executionContext the context the executions of the claim run on
     * @return the new claim
     */
    SegmentClaim startClaim(Segment segment, ProcessingContext executionContext) {
        var claim = new SegmentClaim(segment, executionContext, this::startupHead);
        end(claims.put(segment.getSegmentId(), claim));
        return claim;
    }

    /**
     * Ends the current claim of the given {@code segment}, if any. Returns once an activation of that claim in progress
     * has finished.
     *
     * @param segment the released segment
     */
    void release(Segment segment) {
        end(claims.remove(segment.getSegmentId()));
    }

    private static void end(@Nullable SegmentClaim claim) {
        if (claim != null) {
            claim.end();
        }
    }

    /**
     * Returns the current claim of the given {@code segment}, without recording one when there is none.
     *
     * @param segment the segment to look up
     * @return the current claim of the segment, or empty when none is recorded
     */
    Optional<SegmentClaim> claimOf(Segment segment) {
        return Optional.ofNullable(claims.get(segment.getSegmentId()))
                       .filter(claim -> claim.segment().equals(segment));
    }

    /**
     * Returns whether the current claim of the given {@code segment} failed to restore its executions.
     *
     * @param segment the segment to check, or {@code null} outside a segmented processor
     * @return {@code true} when the segment's current claim failed
     */
    boolean hasFailed(@Nullable Segment segment) {
        var claim = segment == null ? null : claims.get(segment.getSegmentId());
        return claim != null && claim.hasFailed();
    }

    /**
     * Holds the checkpoint of the current claim of the given {@code segment} until that claim ends: the segment failed
     * to handle an event, so neither a checkpoint nor its release may store a position past that event. The next claim
     * of the segment then handles the event again. A segment without a current claim gets an implicit one.
     *
     * @param segment the segment that failed to handle an event
     */
    void holdCheckpoint(Segment segment) {
        currentOrImplicitClaim(segment).holdCheckpoint();
    }

    /**
     * Returns whether the current claim of the given {@code segment} holds its checkpoint: its restore failed, or it
     * failed to handle an event.
     *
     * @param segment the segment to check, or {@code null} outside a segmented processor
     * @return {@code true} when the segment's current claim may store no position
     */
    boolean holdsCheckpoint(@Nullable Segment segment) {
        var claim = segment == null ? null : claims.get(segment.getSegmentId());
        return claim != null && claim.holdsCheckpoint();
    }

    /**
     * Activates the given {@code claim} when the position its segment resumes from covers the startup head, or else
     * polls the position of its segment until it does, and activates it then.
     *
     * @param claim       the claim that restored its executions
     * @param claimedFrom the stored position the segment resumes from, or {@code null} when it has consumed nothing
     * @param activation  starts the restored executions of the claim once it caught up
     */
    void activateOrAwaitCatchUp(SegmentClaim claim, @Nullable TrackingToken claimedFrom, Activation activation) {
        Objects.requireNonNull(activation, "The activation must not be null.");
        var segment = claim.segment();
        boolean activated = claim.activate(
                claimedFrom, () -> activation.start(segment, "on claim of segment " + segment.getSegmentId())
        );
        if (!activated) {
            logger.info("Segment {} is catching up; restored workflow executions start after catch-up.",
                        segment.getSegmentId());
            awaitCatchUp(claim, activation, 0);
        }
    }

    /**
     * Polls the position of the segment of the given {@code claim} until it covers the startup head.
     * <p>
     * A segment normally learns it caught up from an event it handles at or after the startup head. But the processor
     * skips events of other segments without handing them over, so a segment whose backlog holds no event of its own
     * would never learn it, and its restored workflows would never start. Its position, which the processor advances
     * past skipped events as well, tells it instead. Polling ends once the claim caught up, either way, or ended; a
     * poller of an ended claim can no longer activate anything. It also ends once the position of the segment was
     * unknown for {@link #CATCH_UP_POLLS_WITHOUT_POSITION} polls in a row: the processor no longer holds the segment,
     * for example because it was claimed elsewhere. A position that is briefly unknown, right after the claim, does
     * not end it. An event the segment handles still activates the claim after polling ended.
     *
     * @param claim                the claim waiting to catch up
     * @param activation           starts the restored executions of the claim once it caught up
     * @param pollsWithoutPosition the number of consecutive polls so far that found no position of the segment
     */
    private void awaitCatchUp(SegmentClaim claim, Activation activation, int pollsWithoutPosition) {
        catchUpPolls.accept(() -> {
            if (stopped || !claim.isWaiting()) {
                return;
            }
            var segment = claim.segment();
            int withoutPosition = pollsWithoutPosition;
            try {
                var position = segmentPositions.apply(segment);
                if (position.isPresent()) {
                    withoutPosition = 0;
                    claim.activate(position.get(), () -> activation.start(segment, "after catch-up"));
                } else {
                    withoutPosition++;
                }
            } catch (RuntimeException e) {
                logger.warn("Could not read the position of segment {} while it catches up.",
                            segment.getSegmentId(), e);
            }
            if (withoutPosition >= CATCH_UP_POLLS_WITHOUT_POSITION) {
                logger.info("Segment {} is no longer listed by the processor; no longer polling its position to "
                                    + "catch up.",
                            segment.getSegmentId());
                return;
            }
            awaitCatchUp(claim, activation, withoutPosition);
        });
    }

    /**
     * Activates the current claim of the given {@code segment} when the handled {@code deliveredToken} covers the
     * startup head and the claim was not activated yet. A segment without a current claim gets an implicit one.
     *
     * @param segment        the segment handling an event, or {@code null} outside a segmented processor
     * @param deliveredToken the token of the handled event, or {@code null} when unknown
     * @param activation     starts the restored executions of the claim when it caught up
     */
    void activateOnDelivery(@Nullable Segment segment, @Nullable TrackingToken deliveredToken, Activation activation) {
        Objects.requireNonNull(activation, "The activation must not be null.");
        if (segment == null || deliveredToken == null) {
            return;
        }
        currentOrImplicitClaim(segment).activate(deliveredToken, () -> activation.start(segment, "after catch-up"));
    }

    /**
     * Runs the given {@code action} under the lock of the current claim of the given {@code segment}, telling it when
     * workflow bodies start, whether or not that claim is still current, see
     * {@link SegmentClaim#runAlwaysWithBodyStart(Consumer)}. An action that creates an execution and starts it only
     * when told {@link BodyStart#NOW now} thereby never races an activation that starts every execution it finds. A
     * segment without a current claim gets an implicit one.
     *
     * @param segment the segment handling an event, or {@code null} outside a segmented processor, where bodies always
     *                start {@link BodyStart#NOW now}
     * @param action  receives when workflow bodies start
     */
    void runAlwaysWithBodyStart(@Nullable Segment segment, Consumer<BodyStart> action) {
        if (segment == null) {
            action.accept(BodyStart.NOW);
            return;
        }
        currentOrImplicitClaim(segment).runAlwaysWithBodyStart(action);
    }

    /**
     * Returns the current claim of the given {@code segment}, recording an implicit claim, without execution context,
     * when the segment has none.
     */
    private SegmentClaim currentOrImplicitClaim(Segment segment) {
        return claims.computeIfAbsent(segment.getSegmentId(),
                                      id -> new SegmentClaim(segment, null, this::startupHead));
    }

    private @Nullable TrackingToken startupHead() {
        return startupHead;
    }

    /**
     * Starts the restored workflow executions of a segment whose claim caught up with the startup head. Runs under the
     * lock of that claim, at most once per claim. The engine hands it to every call that may activate a claim, so the
     * call site shows what an activation runs.
     */
    @FunctionalInterface
    interface Activation {

        /**
         * Starts the restored workflow executions of the given {@code segment}.
         *
         * @param segment the segment whose claim caught up
         * @param phase   describes when the claim caught up, for logging
         */
        void start(Segment segment, String phase);
    }
}
