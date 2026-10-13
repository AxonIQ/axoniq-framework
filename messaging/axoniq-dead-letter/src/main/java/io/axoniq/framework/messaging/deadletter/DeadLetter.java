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

import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.WrappedToken;

import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.UnaryOperator;

/**
 * Interface describing a dead lettered {@link Message} implementation of generic type {@code M}.
 * <p>
 * The time of storing the {@link #message()} is kept through {@link #enqueuedAt()}. The last time this letter was
 * accessed on either
 * {@link SequencedDeadLetterQueue#requeue(DeadLetter, UnaryOperator,
 * org.axonframework.messaging.core.unitofwork.ProcessingContext)} or
 * {@link SequencedDeadLetterQueue#process(java.util.function.Predicate, java.util.function.Function,
 * org.axonframework.messaging.core.unitofwork.ProcessingContext) processing}, is kept in {@link #lastTouched()}.
 * Additional information on why the letter is enqueued can be found in the {@link #diagnostics() diagnostics}.
 *
 * @param <M> The type of {@link Message} represented by this interface.
 * @author Steven van Beelen
 * @author Allard Buijze
 * @since 4.6.0
 */
public interface DeadLetter<M extends Message> {

    /**
     * The {@link Context.ResourceKey} used whenever a {@link Context} would contain a {@code DeadLetter}.
     */
    Context.ResourceKey<DeadLetter<?>> RESOURCE_KEY = Context.ResourceKey.withLabel("deadLetter");

    /**
     * Returns an {@link Optional} of {DeadLetter}, returning the resource keyed under the {@link #RESOURCE_KEY} in the
     * given {@code context}.
     *
     * @param context the {@link Context} to retrieve the {@code DeadLetter} from, if present
     * @return an {@link Optional} of {DeadLetter}, returning the resource keyed under the {@link #RESOURCE_KEY} in the
     * given {@code context}
     */
    static Optional<DeadLetter<?>> fromContext(Context context) {
        return Optional.ofNullable(context.getResource(RESOURCE_KEY));
    }

    /**
     * The {@link Message} of type {@code M} contained in this letter.
     *
     * @return the {@link Message} of type {@code M} contained in this letter
     */
    M message();

    /**
     * The {@link Cause cause} for the {@link #message()} to be dead lettered. Can be an {@link Optional#empty()} in
     * case this letter is enqueued without a causal error. For instance, when another letter already present in the
     * queue was blocking it being handled.
     *
     * @return the {@link Cause cause} for the {@link #message()} to be dead lettered
     */
    Optional<Cause> cause();

    /**
     * The moment in time when the {@link #message()} was entered in a dead letter queue.
     *
     * @return the moment in time when the {@link #message()} was entered in a dead letter queue
     */
    Instant enqueuedAt();

    /**
     * The moment in time when this letter was last touched. Will equal the {@link #enqueuedAt()} value if this letter
     * is enqueued for the first time.
     *
     * @return the moment in time when this letter was last touched
     */
    Instant lastTouched();

    /**
     * The diagnostic {@link Metadata} concerning this letter.
     *
     * @return the diagnostic {@link Metadata} concerning this letter
     */
    Metadata diagnostics();

    /**
     * Construct a copy of this {@code DeadLetter}, replacing the {@link #lastTouched()} with the current time.
     *
     * @return a copy of this {@code DeadLetter} with {@link #lastTouched()} set to now
     */
    DeadLetter<M> markTouched();

    /**
     * Construct a copy of this {@code DeadLetter}, replacing the {@link #cause()} with the given {@code requeueCause}.
     *
     * @param requeueCause the new cause of the {@code DeadLetter} under construction
     * @return a copy of this {@code DeadLetter}, replacing the {@link #cause()} with the given {@code requeueCause}
     */
    DeadLetter<M> withCause(Throwable requeueCause);

    /**
     * Construct a copy of this {@code DeadLetter}, replacing the {@link DeadLetter#diagnostics()} with the given
     * {@code diagnostics}.
     *
     * @param diagnostics the diagnostic {@link Metadata} to append to the {@code DeadLetter} under construction
     * @return a copy of this {@code DeadLetter}, replacing the {@link DeadLetter#diagnostics()} with the given
     * {@code diagnostics}
     */
    DeadLetter<M> withDiagnostics(Metadata diagnostics);

    /**
     * Construct a copy of this {@code DeadLetter}, replacing the {@link DeadLetter#diagnostics()} with the result of
     * the given {@code diagnosticsBuilder}. The {@code diagnosticsBuilder} will be invoked with the diagnostics of this
     * instance.
     *
     * @param diagnosticsBuilder the {@link UnaryOperator lambda} constructing diagnostic {@link Metadata} to replace
     *                           the {@code Metadata} of the {@code DeadLetter} under construction.
     * @return a copy of this {@code DeadLetter}, replacing the {@link DeadLetter#diagnostics()} with the result of the
     * given {@code diagnosticsBuilder}
     */
    default DeadLetter<M> withDiagnostics(UnaryOperator<Metadata> diagnosticsBuilder) {
        return withDiagnostics(diagnosticsBuilder.apply(this.diagnostics()));
    }

    /**
     * Returns the {@link Context} associated with this dead letter, carrying resources that were present in the
     * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} when the letter was enqueued. Defaults to
     * {@link Context#empty()} if not overridden.
     * <p>
     * The following resources are stored and restored through this context:
     * <ul>
     *     <li>{@link org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken#RESOURCE_KEY}:
     *     always present when the letter was enqueued by a streaming event processor.</li>
     *     <li>{@link org.axonframework.messaging.core.LegacyResources#AGGREGATE_TYPE_KEY},
     *     {@link org.axonframework.messaging.core.LegacyResources#AGGREGATE_IDENTIFIER_KEY}, and
     *     {@link org.axonframework.messaging.core.LegacyResources#AGGREGATE_SEQUENCE_NUMBER_KEY}, present only when
     *     the event was stored by an aggregate-based event storage engine.</li>
     * </ul>
     *
     * @return the {@link Context} associated with this dead letter
     */
    default Context context() {
        return Context.empty();
    }

    /**
     * Returns the position of this dead letter in the event stream it originated on, based on the {@link TrackingToken}
     * in its {@link #context() context}, when present.
     * <p>
     * A {@link WrappedToken} is unwrapped to the position of the underlying event.
     *
     * @return the position of this dead letter, or empty when its {@link #context() context} holds no
     * {@code TrackingToken}, or that token has no position
     */
    default OptionalLong position() {
        return context().getResource(TrackingToken.RESOURCE_KEY) instanceof TrackingToken token
                ? WrappedToken.unwrapLowerBound(token).position()
                : OptionalLong.empty();
    }
}
