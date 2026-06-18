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

package io.axoniq.framework.statecontroller.conditions;

import org.axonframework.common.FutureUtils;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A deferred question about a slice of event history that produces a single value of type {@code T} when forced.
 * <p>
 * Conditions are the unit of composition in a State Controller decision. They are declared procedurally but not
 * evaluated until {@link #asCompletableFuture()} (or its synchronous bridge {@link #resolve()}, or a specialized
 * evaluator such as {@link BooleanCondition#resolve()}) is invoked. Declaring a condition records intent;
 * evaluating it triggers a single coordinated load of the underlying events for the enclosing scope. Holding a
 * {@code Condition} reference is therefore cheap and free of side effects.
 * <p>
 * The asynchronous shape — {@link #asCompletableFuture()} returning a {@link CompletableFuture} — is the primary
 * operation. {@link #resolve()} is the synchronous force verb that bridges the future back to the caller via
 * {@link FutureUtils#joinAndUnwrap(CompletableFuture, Duration)}; this preserves the imperative style of a
 * decision body while keeping the underlying loading lifecycle non-blocking. Operators ({@link #map},
 * {@link #zip}) are implemented as decorators that chain {@code thenApply}/{@code thenCombine} on the upstream
 * future, never re-blocking on intermediate results.
 * <p>
 * Specialized subtypes ({@link BooleanCondition}, {@link NumericCondition}, {@link OptionalCondition}) add
 * operations that only make sense for their shape and return the specialized type so chains stay fluent without
 * re-wrapping. Use {@link #map(Function)} and {@link #zip(Condition, BiFunction)} when you need to combine
 * values whose shape does not fit one of the specialized subtypes.
 *
 * @param <T> the type of value produced when this condition is forced
 * @author Allard Buijze
 * @since 5.2.0
 */
public interface Condition<T> {

    /**
     * Default safety-net timeout used by {@link #resolve()} when bridging the asynchronous result back to a
     * synchronous caller. Thirty seconds is long enough that no realistic loaded scope should hit it under
     * healthy conditions, but short enough that pathological cases (deadlocks, partitioned event stores)
     * surface as failures rather than thread leaks.
     */
    Duration CONDITION_LOAD_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Returns a {@link CompletableFuture} that completes with this condition's value once the underlying
     * loaded events have been observed.
     * <p>
     * Forcing this method on any condition produced from a given
     * {@link io.axoniq.framework.statecontroller.eventstream.EventStream EventStream} triggers a single sourced
     * read against the event store covering every event type declared on the stream up to that moment, and
     * seals the stream. Subsequent calls — on this condition or any other condition from the same stream —
     * reuse the loaded events; each condition exposes its own future, all completed by the same underlying
     * reduce. Sealing is per-stream: forcing a condition on one scope does not load or seal any other scope.
     * <p>
     * The returned future may already be completed when this method returns (when an upstream condition has
     * already been forced) or pending (the load is in flight); callers should treat both cases uniformly.
     *
     * @return a future that completes with this condition's value
     */
    CompletableFuture<T> asCompletableFuture();

    /**
     * Forces this condition synchronously, returning its value.
     * <p>
     * This is the synchronous force verb on a {@code Condition}: it triggers the underlying coordinated load (if
     * not already in flight) and blocks until the value is available. The default implementation bridges
     * {@link #asCompletableFuture()} back to a synchronous result through
     * {@link FutureUtils#joinAndUnwrap(CompletableFuture, Duration)} with the
     * {@link #CONDITION_LOAD_TIMEOUT default safety-net timeout}, preserving the original exception type if
     * the underlying load fails. Implementations with a cheaper synchronous path may override.
     *
     * @return the value produced by this condition
     */
    default T resolve() {
        return FutureUtils.joinAndUnwrap(asCompletableFuture(), CONDITION_LOAD_TIMEOUT);
    }

    /**
     * Forces this condition synchronously, returning its value.
     *
     * @return the value produced by this condition
     * @deprecated in favour of {@link #resolve()}, the force verb on {@code Condition}; this method now delegates
     * to {@link #resolve()}
     */
    @Deprecated
    default T value() {
        return resolve();
    }

    /**
     * Returns a new condition that applies {@code fn} to this condition's value.
     * <p>
     * The returned condition does not register any accumulator with the backing event stream; its future is
     * {@code this.asCompletableFuture().thenApply(fn)}. Chained {@code map} calls fuse into a single
     * projection.
     *
     * @param fn  the transformation to apply when this condition is forced
     * @param <U> the target value type
     * @return a condition producing {@code fn(value())}
     */
    default <U> Condition<U> map(Function<? super T, ? extends U> fn) {
        Objects.requireNonNull(fn, "fn must not be null");
        return new MappedCondition<>(this, fn);
    }

    /**
     * Combines this condition with {@code other} using {@code combiner}, producing a single condition over the
     * joined values. Both source conditions are evaluated together when the resulting condition is forced.
     * <p>
     * The returned condition does not register any accumulator with a backing event stream; its future is
     * {@code this.asCompletableFuture().thenCombine(other.asCompletableFuture(), combiner)}. When {@code other}
     * comes from the same scope as this condition, both futures complete from the same unified load; when it
     * comes from a different scope (or is not stream-backed at all), each side completes independently and
     * {@code thenCombine} resolves as soon as both are ready.
     *
     * @param other    the other condition to combine with
     * @param combiner the function that joins both values into the result
     * @param <U>      the value type of {@code other}
     * @param <R>      the result value type
     * @return a condition producing {@code combiner(value(), other.value())}
     */
    default <U, R> Condition<R> zip(Condition<U> other, BiFunction<? super T, ? super U, ? extends R> combiner) {
        Objects.requireNonNull(other, "other must not be null");
        Objects.requireNonNull(combiner, "combiner must not be null");
        return new ZippedCondition<>(this, other, combiner);
    }
}
