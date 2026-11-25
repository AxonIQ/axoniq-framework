/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * One claim of a workflow-engine segment: whether it has consumed the startup backlog, whether it is still current, and
 * whether it holds its checkpoint.
 * <p>
 * Catch-up state belongs to a claim, not to a segment id: every claim starts out {@link State#CATCHING_UP behind}, so
 * state left by an earlier claim of the same segment, or by a poller of that claim still running, never makes a later
 * claim skip its activation. The {@link #activate(TrackingToken, Runnable) activation} of a claim, which starts its
 * restored workflow executions, runs exactly once, and every action of the claim runs under the claim's lock, so the
 * start of a new instance and the activation that starts every restored execution never both start the same execution.
 * <p>
 * A claim whose restore failed is inert: it is never activated, and the executions its restore still adds afterwards
 * are dropped, as are those of a claim that ended. Adding them runs under the claim's lock as well, so ending or
 * failing a claim never interleaves with it.
 * <p>
 * The state of a claim is guarded by its own lock, which is no monitor: workflow threads are virtual threads, which
 * take other locks and wait for timers while they hold it. A virtual thread that blocks while it holds a monitor, or
 * while it waits to enter one, keeps its carrier thread. Once every carrier is kept that way, the virtual thread they
 * wait for never runs again.
 * <p>
 * Where the engine takes both, it takes the lock of a workflow before the lock of a claim, see the lock order of
 * {@link LiveWorkflows}. An action of a claim therefore takes no lock of a workflow: the hibernated workflows of a
 * failed claim are dropped outside its lock, and the cancellation requests of an execution that leaves memory under it
 * are answered on another thread.
 * <p>
 * The {@link SegmentClaims} of the engine create and end the claims; the engine and its hibernation support run their
 * actions on them.
 *
 * @author Simon Zambrovski
 * @author Allard Buijze
 * @since 5.4.0
 */
@Internal
final class SegmentClaim {

    /**
     * The lifecycle of a claim. A claim starts {@link #CATCHING_UP}, becomes {@link #ACTIVE} once it caught up with the
     * startup head, and ends {@link #ENDED} when a later claim of its segment started or the segment was released. It
     * {@link #FAILED fails} when its restore failed, from any state, and stays failed when it ends. Only a claim that
     * fails while current runs the cleanup of its failure.
     */
    enum State {

        /**
         * Current and behind the startup head: its restored executions wait for its activation to start.
         */
        CATCHING_UP,
        /**
         * Current and caught up with the startup head: its activation ran.
         */
        ACTIVE,
        /**
         * Its restore failed: it runs no action and holds its checkpoint. A failed claim that ends stays failed, so a
         * thread that still holds it keeps seeing it failed.
         */
        FAILED,
        /**
         * A later claim of its segment started, or the segment was released: it runs no action anymore.
         */
        ENDED;

        private boolean isCurrent() {
            return this == CATCHING_UP || this == ACTIVE;
        }
    }

    /**
     * When the body of a workflow execution that enters memory under a claim starts: right away, by the action that
     * brings it into memory, or later, by the activation of the claim. Never both, as the action and the activation
     * run under the lock of the claim, and the activation runs once.
     */
    enum BodyStart {

        /**
         * The claim activated, or there is no startup head to catch up with: the action that brings the execution
         * into memory starts its body itself.
         */
        NOW,
        /**
         * The action leaves the body alone. Either the claim still catches up, and its activation starts the body, as
         * it starts every execution it finds that does not run yet, or the claim ended or failed, and the body does not
         * start under it.
         */
        AT_ACTIVATION
    }

    private final ReentrantLock lock = new ReentrantLock();

    private final Segment segment;
    private final @Nullable ProcessingContext executionContext;
    private final Supplier<@Nullable TrackingToken> startupHead;
    private State state = State.CATCHING_UP;
    // Orthogonal to the lifecycle: set once the segment failed to handle an event, until the claim ends.
    private boolean checkpointHeld;

    /**
     * Creates a claim of the given {@code segment} that is catching up.
     *
     * @param segment          the claimed segment
     * @param executionContext the context the executions of the claim run on, or {@code null} for an implicit claim
     * @param startupHead      returns the position a claim must reach before it activates, or {@code null} when every
     *                         claim may start the bodies of its workflows right away
     */
    SegmentClaim(Segment segment,
                 @Nullable ProcessingContext executionContext,
                 Supplier<@Nullable TrackingToken> startupHead) {
        this.segment = segment;
        this.executionContext = executionContext;
        this.startupHead = startupHead;
    }

    /**
     * Returns the claimed segment.
     *
     * @return the claimed segment
     */
    Segment segment() {
        return segment;
    }

    /**
     * Returns the context the executions of this claim run on.
     *
     * @return the execution context of the claim, or {@code null} for an implicit claim, recorded by a segment that
     * handles events without having been claimed through the engine, see {@link SegmentClaims}
     */
    @Nullable ProcessingContext executionContext() {
        return executionContext;
    }

    /**
     * Runs the given {@code action} under the lock of this claim, but only while this claim is current: neither ended
     * nor failed.
     *
     * @param action the action to run
     * @return {@code true} when the action ran, {@code false} when the claim ended or failed
     */
    boolean runIfCurrent(Runnable action) {
        lock.lock();
        try {
            if (!state.isCurrent()) {
                return false;
            }
            action.run();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Runs the given {@code action} under the lock of this claim, telling it when workflow bodies start, but only while
     * this claim is current: neither ended nor failed. Bodies start {@link BodyStart#NOW now} once the claim activated,
     * or right away when there is no startup head, also before the activation ran. An action that starts an execution
     * only when told {@link BodyStart#NOW now} thereby never races the activation of the claim, which starts every
     * execution it finds.
     *
     * @param action receives when workflow bodies start
     * @return {@code true} when the action ran, {@code false} when the claim ended or failed
     */
    boolean runIfCurrentWithBodyStart(Consumer<BodyStart> action) {
        var head = startupHead.get();
        lock.lock();
        try {
            if (!state.isCurrent()) {
                return false;
            }
            action.accept(bodyStart(head));
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Runs the given {@code action} under the lock of this claim, telling it when workflow bodies start, whether or not
     * this claim is current: also when it ended or failed. Bodies start {@link BodyStart#NOW now} once the claim
     * activated and while it is current, or whenever there is no startup head. An action that creates an execution and
     * starts it only when told {@link BodyStart#NOW now} thereby never races an activation that starts every execution
     * it finds.
     *
     * @param action receives when workflow bodies start
     */
    void runAlwaysWithBodyStart(Consumer<BodyStart> action) {
        lock.lock();
        try {
            action.accept(bodyStart(startupHead.get()));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns when the body of an execution entering memory under this claim starts, given the startup head. The caller
     * holds the lock of the claim.
     */
    private BodyStart bodyStart(@Nullable TrackingToken head) {
        return head == null || state == State.ACTIVE ? BodyStart.NOW : BodyStart.AT_ACTIVATION;
    }

    /**
     * Activates this claim when the given {@code position} of its segment covers the startup head, unless it ended,
     * failed or was activated already. The {@code activation} runs at most once per claim, under the claim's lock.
     * Without a startup head, or without a known position, the claim activates right away.
     *
     * @param position   the position the segment consumed up to, or {@code null} when it has consumed nothing
     * @param activation starts the restored executions of the claim
     * @return {@code false} when the claim is behind the startup head and waits to catch up, {@code true} otherwise
     */
    boolean activate(@Nullable TrackingToken position, Runnable activation) {
        var head = startupHead.get();
        lock.lock();
        try {
            if (state != State.CATCHING_UP) {
                return true;
            }
            if (head != null && position != null && !position.covers(head)) {
                return false;
            }
            state = State.ACTIVE;
            activation.run();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns whether this claim still waits to catch up: it is current and not activated yet.
     *
     * @return {@code true} when the claim still waits to catch up
     */
    boolean isWaiting() {
        lock.lock();
        try {
            return state == State.CATCHING_UP;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Marks this claim as failed, and runs the given {@code cleanup} under its lock, but only while the claim is current:
     * a claim that ended or failed already runs no cleanup, so the cleanup runs at most once, and never once a next
     * claim of the segment started. A failed claim is never activated, and runs no
     * {@link #runIfCurrent(Runnable) action} anymore. Returns once an action or activation of the claim in progress
     * has finished. A next claim of the segment only starts once the cleanup finished, so the cleanup never touches
     * what that claim restores.
     *
     * @param cleanup drops what the claim restored
     */
    void fail(Runnable cleanup) {
        lock.lock();
        try {
            boolean current = state.isCurrent();
            state = State.FAILED;
            if (current) {
                cleanup.run();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns whether this claim failed to restore its executions.
     *
     * @return {@code true} when the claim failed
     */
    boolean hasFailed() {
        lock.lock();
        try {
            return state == State.FAILED;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Holds the checkpoint of this claim until it ends: its segment failed to handle an event, so neither a checkpoint
     * nor its release may store a position past that event. The next claim of the segment then handles the event
     * again.
     */
    void holdCheckpoint() {
        lock.lock();
        try {
            checkpointHeld = true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns whether this claim holds its checkpoint: its restore failed, or its segment failed to handle an event.
     *
     * @return {@code true} when the claim may store no position
     */
    boolean holdsCheckpoint() {
        lock.lock();
        try {
            return state == State.FAILED || checkpointHeld;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Ends this claim, as a later claim of its segment started or the segment was released. Waits for an action or
     * activation holding the lock of the claim; later ones see the claim is no longer current. A failed claim stays
     * failed.
     */
    void end() {
        lock.lock();
        try {
            if (state != State.FAILED) {
                state = State.ENDED;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns whether the given thread waits for the lock of this claim, which an activation or another action of the
     * claim holds. Package-private so a test can tell that a thread is held up by that lock, instead of waiting a while
     * for what the thread would do without it.
     *
     * @param thread the thread that may wait for the lock
     * @return {@code true} when the thread waits to acquire the lock of the claim
     */
    boolean isWaitingForLock(Thread thread) {
        return lock.hasQueuedThread(thread);
    }
}
