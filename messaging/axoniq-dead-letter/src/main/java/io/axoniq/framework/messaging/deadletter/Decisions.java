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

package io.axoniq.framework.messaging.deadletter;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.Metadata;

import java.util.function.Function;

/**
 * Utility class providing a number of reasonable {@link EnqueueDecision EnqueueDecisions}. Can, for example, be used by
 * an {@link EnqueuePolicy} to return a decision.
 * <p>
 * Note that the {@code EnqueueDecisions} are <em>only</em> used for deciding if to enqueue or requeue a letter, and
 * nothing more.
 *
 * @author Steven van Beelen
 * @see EnqueuePolicy
 * @since 4.6.0
 */
public final class Decisions {

    /**
     * Construct an {@link Ignore} defining that a {@link DeadLetter dead letter} should remain in the queue.
     * <p>
     * Note that the result is <em>only</em> used to define the letter should remain in the queue, and nothing more.
     *
     * @param <M> The type of message contained in the {@link DeadLetter} that's been made a decision on.
     * @return An {@link Ignore} defining that a {@link DeadLetter dead letter} should remain in the queue.
     */
    public static <M extends Message> Ignore<M> ignore() {
        return new Ignore<>();
    }

    /**
     * Construct a {@link DoNotEnqueue} defining that a {@link DeadLetter dead letter} should not be enqueued at all.
     * <p>
     * Note that the result is <em>only</em> used to define the letter should not be enqueued, and nothing more.
     *
     * @param <M> The type of message contained in the {@link DeadLetter} that's been made a decision on.
     * @return A {@link DoNotEnqueue} defining that a {@link DeadLetter dead letter} should not be enqueued at all.
     */
    public static <M extends Message> DoNotEnqueue<M> doNotEnqueue() {
        return new DoNotEnqueue<>();
    }

    /**
     * Construct a {@link DoNotEnqueue} defining that a {@link DeadLetter dead letter} should be evicted from the
     * queue.
     * <p>
     * Note that the result is <em>only</em> used to define the letter should be evicted from the queue, and nothing
     * more.
     *
     * @param <M> The type of message contained in the {@link DeadLetter} that's been made a decision on.
     * @return A {@link DoNotEnqueue} defining that a {@link DeadLetter dead letter} should be evicted from the queue.
     */
    public static <M extends Message> DoNotEnqueue<M> evict() {
        return new DoNotEnqueue<>();
    }

    /**
     * Construct a {@link ShouldEnqueue} defining that a {@link DeadLetter dead letter} should be enqueued.
     * <p>
     * Note that the result is <em>only</em> used to define the letter should be enqueued, and nothing more.
     *
     * @param <M> The type of message contained in the {@link DeadLetter} that's been made a decision on.
     * @return A {@link ShouldEnqueue} defining that a {@link DeadLetter dead letter} should be enqueued.
     */
    public static <M extends Message> ShouldEnqueue<M> enqueue() {
        return enqueue(null);
    }

    /**
     * Construct a {@link ShouldEnqueue} defining that a {@link DeadLetter dead letter} should be enqueued because of
     * the given {@code enqueueCause}.
     * <p>
     * Note that the result is <em>only</em> used to define the letter should be enqueued with the given
     * {@code enqueueCause}, and nothing more.
     *
     * @param enqueueCause The reason for enqueueing a {@link DeadLetter dead letter}.
     * @param <M>          The type of message contained in the {@link DeadLetter} that's been made a decision on.
     * @return A {@link ShouldEnqueue} defining that a {@link DeadLetter dead letter} should be enqueued because of the
     * given {@code enqueueCause}.
     */
    public static <M extends Message> ShouldEnqueue<M> enqueue(Throwable enqueueCause) {
        return enqueue(enqueueCause, DeadLetter::diagnostics);
    }

    /**
     * Construct a {@link ShouldEnqueue} defining that a {@link DeadLetter dead letter} should be enqueued because of
     * the given {@code enqueueCause}. The {@code diagnosticsBuilder} constructs
     * {@link DeadLetter#diagnostics() diagnostic} {@link Metadata} to append to the letter to enqueue.
     * <p>
     * Note that the result is <em>only</em> used to define the letter should be enqueued with the given
     * {@code enqueueCause} and diagnostics, and nothing more.
     *
     * @param enqueueCause       The reason for enqueueing a {@link DeadLetter dead letter}.
     * @param diagnosticsBuilder A builder of {@link DeadLetter#diagnostics() diagnostic} {@link Metadata}.
     * @param <M>                The type of message contained in the {@link DeadLetter} that's been made a decision
     *                           on.
     * @return A {@link ShouldEnqueue} defining that a {@link DeadLetter dead letter} should be enqueued because of the
     * given {@code enqueueCause}.
     */
    public static <M extends Message> ShouldEnqueue<M> enqueue(
            Throwable enqueueCause,
            Function<DeadLetter<? extends M>, Metadata> diagnosticsBuilder
    ) {
        return new ShouldEnqueue<>(enqueueCause, diagnosticsBuilder);
    }

    /**
     * Construct a {@link ShouldEnqueue} defining that a {@link DeadLetter dead letter} should be requeued because of
     * the given {@code requeueCause}.
     * <p>
     * Note that the result is <em>only</em> used to define the letter should be requeued with the given
     * {@code requeueCause}, and nothing more.
     *
     * @param requeueCause The reason for requeueing a {@link DeadLetter dead letter}.
     * @param <M>          The type of message contained in the {@link DeadLetter} that's been made a decision on.
     * @return A {@link ShouldEnqueue} defining that a {@link DeadLetter dead letter} should be requeued because of the
     * given {@code requeueCause}.
     */
    public static <M extends Message> ShouldEnqueue<M> requeue(Throwable requeueCause) {
        return requeue(requeueCause, DeadLetter::diagnostics);
    }

    /**
     * Construct a {@link ShouldEnqueue} defining that a {@link DeadLetter dead letter} should be requeued because of
     * the given {@code requeueCause}. The {@code diagnosticsBuilder} constructs
     * {@link DeadLetter#diagnostics() diagnostic} {@link Metadata} to append to the letter to requeue.
     * <p>
     * Note that the result is <em>only</em> used to define the letter should be requeued with the given
     * {@code requeueCause} and diagnostics, and nothing more.
     *
     * @param requeueCause       The reason for requeueing a {@link DeadLetter dead letter}.
     * @param diagnosticsBuilder A builder of {@link DeadLetter#diagnostics() diagnostic} {@link Metadata}.
     * @param <M>                The type of message contained in the {@link DeadLetter} that's been made a decision
     *                           on.
     * @return A {@link ShouldEnqueue} defining that a {@link DeadLetter dead letter} should be requeued because of the
     * given {@code requeueCause}.
     */
    public static <M extends Message> ShouldEnqueue<M> requeue(
            Throwable requeueCause,
            Function<DeadLetter<? extends M>, Metadata> diagnosticsBuilder
    ) {
        return new ShouldEnqueue<>(requeueCause, diagnosticsBuilder);
    }

    private Decisions() {
        // Utility class
    }
}
