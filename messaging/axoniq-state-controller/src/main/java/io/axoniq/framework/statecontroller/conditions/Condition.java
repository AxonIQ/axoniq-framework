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
 * A deferred question about a slice of event history that produces a single value of type {@code T} when
 * resolved.
 * <p>
 * Conditions are the unit of composition in a state-controlled decision, whether declared through the
 * {@link io.axoniq.framework.statecontroller.History History} reads or through the lower-level
 * {@link io.axoniq.framework.statecontroller.eventstream.EventStream EventStream} surface. Declaring a condition
 * records intent and touches no I/O; holding a {@code Condition} reference is cheap and free of side effects.
 * The <em>first</em> resolution — {@link #resolve()} for the imperative style, {@link #resolveAsync()} for the
 * reactive style — seals every condition declared so far, across all scopes of the in-flight decision, into a
 * single coordinated event-store read whose criteria narrow to exactly the tags and event types those conditions
 * touch. Every declared condition then resolves from that one read; subsequent resolutions are free.
 * <p>
 * Conditions declared <em>after</em> the first resolution remain valid: they are answered by a supplementary
 * read on the same event-store transaction, preserving one consistent view. The cost model is what changes —
 * each late batch adds a round-trip — so the idiomatic decision body declares its conditions first, then
 * resolves:
 * <pre>{@code
 * History account = history.of("account", cmd.accountId());
 * var closed  = account.has(AccountClosed.class);                      // declares — no I/O
 * var balance = account.total(MoneyDeposited.class, MoneyDeposited::amount)
 *                      .minus(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));
 *
 * if (closed.resolve())                              return reject("account closed");   // ONE combined read
 * if (balance.resolve().compareTo(cmd.amount()) < 0) return reject("insufficient funds"); // already resolved
 * }</pre>
 * <p>
 * The asynchronous shape is primary: {@link #resolveAsync()} never blocks, and {@link #resolve()} is the
 * edge-of-system bridge for imperative bodies, joining the same future with a
 * {@link #RESOLVE_TIMEOUT safety-net timeout}. Operators ({@link #map}, {@link #combine}) are decorators that
 * chain {@code thenApply}/{@code thenCombine} on the upstream future without resolving it, so a fully
 * declarative body can build one derived condition and resolve once.
 * <p>
 * Specialized subtypes ({@link BooleanCondition}, {@link NumericCondition}, {@link OptionalCondition}) add
 * operations that only make sense for their shape and return the specialized type so chains stay fluent without
 * re-wrapping. Use {@link #map(Function)} and {@link #combine(Condition, BiFunction)} when you need to combine
 * values whose shape does not fit one of the specialized subtypes.
 *
 * @param <T> the type of value produced when this condition is resolved
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @since 5.2.0
 */
public interface Condition<T> {

    /**
     * Default safety-net timeout used by {@link #resolve()} when bridging the asynchronous result back to a
     * synchronous caller. Thirty seconds is long enough that no realistic loaded scope should hit it under
     * healthy conditions, but short enough that pathological cases (deadlocks, partitioned event stores)
     * surface as failures rather than thread leaks.
     */
    Duration RESOLVE_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Resolves this condition asynchronously, returning a {@link CompletableFuture} that completes with its
     * value once the underlying events have been observed.
     * <p>
     * The first resolution of any condition in the in-flight decision triggers a single sourced read against the
     * event store covering every scope and event type declared up to that moment, and seals those scopes.
     * Subsequent calls — on this condition or any other declared condition — reuse the loaded events; each
     * condition exposes its own future, all completed by the same underlying read. The returned future may
     * already be completed when this method returns (when another condition has already been resolved) or
     * pending (the load is in flight); callers should treat both cases uniformly.
     *
     * @return a future that completes with this condition's value
     */
    CompletableFuture<T> resolveAsync();

    /**
     * Resolves this condition synchronously, returning its value.
     * <p>
     * This is the imperative edge over {@link #resolveAsync()}: it joins the same future through
     * {@link FutureUtils#joinAndUnwrap(CompletableFuture, Duration)} with the {@link #RESOLVE_TIMEOUT default
     * safety-net timeout}, preserving the original exception type if the underlying load fails. The internal
     * loading lifecycle stays non-blocking; only this call site waits.
     *
     * @return the value produced by this condition
     */
    default T resolve() {
        return FutureUtils.joinAndUnwrap(resolveAsync(), RESOLVE_TIMEOUT);
    }

    /**
     * Returns a new condition that applies {@code mapper} to this condition's value, without resolving it.
     * <p>
     * The returned condition does not register any accumulator with the backing event stream; its future is
     * {@code this.resolveAsync().thenApply(mapper)}. Chained {@code map} calls fuse into a single projection.
     *
     * @param mapper the transformation to apply when this condition is resolved
     * @param <U>    the target value type
     * @return a condition producing {@code mapper(resolve())}
     */
    default <U> Condition<U> map(Function<? super T, ? extends U> mapper) {
        Objects.requireNonNull(mapper, "mapper must not be null");
        return new MappedCondition<>(this, mapper);
    }

    /**
     * Combines this condition with {@code other} using {@code combiner}, producing a single condition over the
     * joined values, without resolving either. Both source conditions resolve together when the resulting
     * condition is resolved.
     * <p>
     * The returned condition does not register any accumulator with a backing event stream; its future is
     * {@code this.resolveAsync().thenCombine(other.resolveAsync(), combiner)}. When {@code other} comes from the
     * same decision, both sides complete from the same coordinated read; when it is not history-backed at all,
     * each side completes independently and the combination resolves as soon as both are ready.
     *
     * @param other    the other condition to combine with
     * @param combiner the function that joins both values into the result
     * @param <U>      the value type of {@code other}
     * @param <R>      the result value type
     * @return a condition producing {@code combiner(resolve(), other.resolve())}
     */
    default <U, R> Condition<R> combine(Condition<U> other, BiFunction<? super T, ? super U, ? extends R> combiner) {
        Objects.requireNonNull(other, "other must not be null");
        Objects.requireNonNull(combiner, "combiner must not be null");
        return new CombinedCondition<>(this, other, combiner);
    }
}
