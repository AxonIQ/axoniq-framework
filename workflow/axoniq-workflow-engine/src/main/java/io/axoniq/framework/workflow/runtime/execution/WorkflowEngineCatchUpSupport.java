/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Tracks, per claim of a workflow-engine segment, whether that claim has consumed the startup backlog, and runs the
 * activation of a claim, which starts its restored workflow executions, exactly once.
 * <p>
 * Catch-up state belongs to a claim, not to a segment id: every claim starts out behind, so state left by an earlier
 * claim of the same segment, or by a poller of that claim still running, never makes a later claim skip its activation.
 * An activation, and the start of a new instance on the handler thread, run under the monitor of their claim, so the
 * start of a new instance and the activation that starts every restored execution never both start the same execution.
 * <p>
 * A claim whose restore failed is inert: it is never activated, and the executions its restore still adds afterwards
 * are dropped, as are those of a claim that ended. Adding them runs under the claim's monitor as well, so ending or
 * failing a claim never interleaves with it.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
final class WorkflowEngineCatchUpSupport {

    private final Map<Integer, Claim> claims = new ConcurrentHashMap<>();
    @Nullable
    private volatile TrackingToken startupHead;

    void initialize(@Nullable TrackingToken startupHead) {
        this.startupHead = startupHead;
    }

    /**
     * Starts a new claim of the given {@code segment}, ending any claim of it that is still recorded. Returns once an
     * activation of that previous claim in progress has finished, so it cannot start executions of the new claim.
     *
     * @param segment the claimed segment
     * @return the new claim
     */
    Claim startClaim(Segment segment) {
        Claim claim = new Claim(segment);
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

    /**
     * Marks the given {@code claim} as failed, and runs the given {@code cleanup} under its monitor unless the claim
     * ended already. A failed claim is never activated, and runs no {@link #runWhileCurrent(Claim, Runnable) action}
     * anymore. Returns once an action or activation of the claim in progress has finished. A next claim of the segment
     * only starts once the cleanup finished, so the cleanup never touches what that claim restores.
     *
     * @param claim   the claim whose restore failed
     * @param cleanup drops what the claim restored
     */
    void fail(Claim claim, Runnable cleanup) {
        synchronized (claim) {
            claim.failed = true;
            if (!claim.ended) {
                cleanup.run();
            }
        }
    }

    /**
     * Returns whether the current claim of the given {@code segment} failed to restore its executions.
     *
     * @param segment the segment to check, or {@code null} outside a segmented processor
     * @return {@code true} when the segment's current claim failed
     */
    boolean hasFailed(@Nullable Segment segment) {
        if (segment == null) {
            return false;
        }
        Claim claim = claims.get(segment.getSegmentId());
        if (claim == null) {
            return false;
        }
        synchronized (claim) {
            return claim.failed;
        }
    }

    /**
     * Holds the checkpoint of the current claim of the given {@code segment} until that claim ends: the segment failed
     * to handle an event, so neither a checkpoint nor its release may store a position past that event. The next claim
     * of the segment then handles the event again.
     *
     * @param segment the segment that failed to handle an event
     */
    void holdCheckpoint(Segment segment) {
        Claim claim = currentClaim(segment);
        synchronized (claim) {
            claim.checkpointHeld = true;
        }
    }

    /**
     * Returns whether the current claim of the given {@code segment} holds its checkpoint: its restore failed, or it
     * failed to handle an event.
     *
     * @param segment the segment to check, or {@code null} outside a segmented processor
     * @return {@code true} when the segment's current claim may store no position
     */
    boolean holdsCheckpoint(@Nullable Segment segment) {
        if (segment == null) {
            return false;
        }
        Claim claim = claims.get(segment.getSegmentId());
        if (claim == null) {
            return false;
        }
        synchronized (claim) {
            return claim.failed || claim.checkpointHeld;
        }
    }

    /**
     * Runs the given {@code action} under the monitor of the given {@code claim}, but only while that claim is current:
     * neither ended nor failed.
     *
     * @param claim  the claim the action belongs to
     * @param action the action to run
     * @return {@code true} when the action ran, {@code false} when the claim ended or failed
     */
    boolean runWhileCurrent(Claim claim, Runnable action) {
        synchronized (claim) {
            if (claim.ended || claim.failed) {
                return false;
            }
            action.run();
            return true;
        }
    }

    private static void end(@Nullable Claim claim) {
        if (claim != null) {
            // Waits for an activation holding the monitor; later ones see the claim is no longer current.
            synchronized (claim) {
                claim.ended = true;
            }
        }
    }

    /**
     * Activates the given {@code claim} when the given {@code position} of its segment covers the startup head, unless
     * it ended, failed or was activated already. The {@code activation} runs at most once per claim, under the claim's
     * monitor.
     *
     * @param claim      the claim to activate
     * @param position   the position the segment consumed up to, or {@code null} when it has consumed nothing
     * @param activation starts the restored executions of the claim
     * @return {@code false} when the claim is behind the startup head and waits to catch up, {@code true} otherwise
     */
    boolean activate(Claim claim, @Nullable TrackingToken position, Runnable activation) {
        var head = startupHead;
        synchronized (claim) {
            if (claim.ended || claim.failed || claim.caughtUp) {
                return true;
            }
            if (head != null && position != null && !position.covers(head)) {
                return false;
            }
            claim.caughtUp = true;
            activation.run();
            return true;
        }
    }

    /**
     * Activates the current claim of the given {@code segment} when the handled {@code deliveredToken} covers the
     * startup head and the claim was not activated yet. A segment handling events without a recorded claim gets one.
     *
     * @param segment        the segment handling an event, or {@code null} outside a segmented processor
     * @param deliveredToken the token of the handled event, or {@code null} when unknown
     * @param activation     starts the restored executions of the segment
     */
    void activateOnDelivery(@Nullable Segment segment, @Nullable TrackingToken deliveredToken, Runnable activation) {
        if (segment == null || deliveredToken == null) {
            return;
        }
        activate(currentClaim(segment), deliveredToken, activation);
    }

    /**
     * Returns whether the given {@code claim} still waits to catch up: it is current and not activated yet.
     *
     * @param claim the claim to check
     * @return {@code true} when the claim still waits to catch up
     */
    boolean isWaiting(Claim claim) {
        synchronized (claim) {
            return !claim.ended && !claim.failed && !claim.caughtUp;
        }
    }

    /**
     * Runs the given {@code action} while no activation of the current claim of the given {@code segment} runs, telling
     * it whether that claim caught up. An action that creates an execution and starts it when caught up thereby never
     * races an activation that starts every execution it finds.
     *
     * @param segment the segment handling an event, or {@code null} outside a segmented processor
     * @param action  receives whether the segment caught up
     */
    void runExclusively(@Nullable Segment segment, Consumer<Boolean> action) {
        if (segment == null) {
            action.accept(true);
            return;
        }
        Claim claim = currentClaim(segment);
        synchronized (claim) {
            action.accept(startupHead == null || (claim.caughtUp && !claim.ended && !claim.failed));
        }
    }

    private Claim currentClaim(Segment segment) {
        return claims.computeIfAbsent(segment.getSegmentId(), id -> new Claim(segment));
    }

    /**
     * One claim of a segment. Its state is guarded by its own monitor.
     */
    static final class Claim {

        private final Segment segment;
        private boolean caughtUp;
        private boolean ended;
        private boolean failed;
        private boolean checkpointHeld;

        private Claim(Segment segment) {
            this.segment = segment;
        }

        /**
         * Returns the claimed segment.
         *
         * @return the claimed segment
         */
        Segment segment() {
            return segment;
        }
    }
}
