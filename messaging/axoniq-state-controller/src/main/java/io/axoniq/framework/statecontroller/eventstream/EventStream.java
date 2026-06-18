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

package io.axoniq.framework.statecontroller.eventstream;

import io.axoniq.framework.statecontroller.conditions.BooleanCondition;
import io.axoniq.framework.statecontroller.conditions.Condition;
import io.axoniq.framework.statecontroller.conditions.MatchBuilder;
import io.axoniq.framework.statecontroller.conditions.NumericCondition;
import io.axoniq.framework.statecontroller.conditions.OptionalCondition;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * A lazy, tag-scoped view over the events relevant to a decision.
 * <p>
 * Obtained from
 * {@link io.axoniq.framework.statecontroller.decisions.DecisionContext#scope DecisionContext.scope(...)},
 * an {@code EventStream} represents the slice of event history covered by one or more tags. Every operation on
 * this interface carries one or more {@code Class<?>} arguments identifying which event types it cares about;
 * those classes are what the framework registers with the loading-context to derive the {@code SourcingCondition}
 * for the underlying event-store query. There is no opaque escape hatch: a decision that needs to read events of
 * type {@code X} must say so by name, which keeps the load minimal and the consistency boundary precise.
 * <p>
 * The general aggregator is {@link #fold(Object) fold}, which returns a {@link FoldableCondition} the caller
 * extends by attaching one typed reducer per event type via
 * {@link FoldableCondition#event(Class, java.util.function.BiFunction) event(...)}. The rest of the surface is
 * sugar for common shapes and follows a uniform pattern: each helper has a single-class <em>typed</em> form
 * and, where it makes sense, a varargs multi-class sibling.
 * <ul>
 *     <li>Predicates: {@link #contains} / {@link #containsAnyOf}</li>
 *     <li>Counting: {@link #count} (varargs over one or more types)</li>
 *     <li>Summing: {@link #sum} for {@link BigDecimal}, {@link #sumLong} for {@code long} (single class only —
 *         a typed mapper requires a known {@code E})</li>
 *     <li>Selecting the last event: {@link #latest} / {@link #latestOf} / {@link #latestMatch}</li>
 *     <li>Selecting the first event: {@link #first} / {@link #firstOf}</li>
 * </ul>
 * Single-class forms preserve the payload's static type through the returned condition. Varargs forms widen to
 * {@link EventCondition} because no single payload class can be assumed; consumers then narrow via
 * {@link EventCondition#isA}, {@link EventCondition#as}, or {@code instanceof} as appropriate. There is no
 * list-valued helper — materializing every matching event into a list is exactly the shape that invites
 * unbounded loads, and accumulation is better expressed as a {@code fold}.
 * <p>
 * No events are loaded when an {@code EventStream} is constructed; loading happens on the first
 * {@link Condition#resolve()} (or specialized evaluator) of any condition derived from it, at which point all
 * conditions declared so far are satisfied in a single coordinated read. The position observed at that read is
 * what later becomes the DCB consistency point used by an {@code Accept} decision.
 * <p>
 * For decision bodies that want a value inline rather than a deferred {@link Condition}, each lazy helper has an
 * eager {@code resolveXxx(...)} sibling ({@link #resolveContains}, {@link #resolveCount}, {@link #resolveSum},
 * {@link #resolveLatest}, {@link #resolveLatestOf}, {@link #resolveFirst}) that builds the condition and forces
 * it in one call. These shortcuts trade batching for convenience: each one issues its own coordinated read
 * immediately, so two {@code resolveXxx(...)} calls on the same scope load twice. Reach for them when a single
 * value is needed at the point of use; declare the lazy {@link Condition}s and force them together when several
 * questions about the same scope should share one read.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
public interface EventStream {

    /**
     * Starts a typed fold over this stream with the given {@code initial} accumulator, returning a
     * {@link FoldableCondition} the caller extends by attaching one
     * {@link FoldableCondition#event(Class, java.util.function.BiFunction) event(...)} reducer per event type.
     * <p>
     * Each chained {@code event(...)} call registers its event type (class or qualified name) with the
     * loading-context and appends a typed reducer; together they form a single fold over the union of registered
     * types in chronological order. Because the result is itself a {@link Condition}, the fold can be forced,
     * mapped, zipped, or extended further at any point in the chain.
     * <p>
     * Example:
     * <pre>{@code
     * var balance = account.fold(BigDecimal.ZERO)
     *     .event(MoneyDeposited.class, (sum, e) -> sum.add(e.amount()))
     *     .event(MoneyWithdrawn.class, (sum, e) -> sum.subtract(e.amount()));
     * }</pre>
     *
     * @param initial the starting accumulator value
     * @param <T>     the accumulator type
     * @return an empty fold builder, ready to receive {@code event(...)} reducers
     */
    <T> FoldableCondition<T> fold(T initial);

    /**
     * Returns a condition that is {@code true} when any event in the stream is an instance of {@code type}.
     *
     * @param type the event payload type to look for
     * @return a {@link BooleanCondition} expressing membership of {@code type}
     */
    BooleanCondition contains(Class<?> type);

    /**
     * Returns a condition that is {@code true} when any event in the stream is an instance of any of the given
     * types.
     *
     * @param types the candidate event payload types
     * @return a {@link BooleanCondition} expressing membership of any of {@code types}
     */
    BooleanCondition containsAnyOf(Class<?>... types);

    /**
     * Counts the events in the stream whose payload is an instance of any of the given {@code types}.
     *
     * @param types the event payload types to count; at least one required
     * @return a {@link NumericCondition} producing the combined count across all matching types
     */
    NumericCondition<Long> count(Class<?>... types);

    /**
     * Sums the projection {@code mapper} applied to every event in the stream that is an instance of {@code type}.
     *
     * @param type   the event payload type to sum over
     * @param mapper the projection from each event to a {@link BigDecimal} addend
     * @param <E>    the event payload type
     * @return a {@link NumericCondition} producing the total
     */
    <E> NumericCondition<BigDecimal> sum(Class<E> type, Function<? super E, BigDecimal> mapper);

    /**
     * Sums the projection {@code mapper} applied to every event in the stream that is an instance of {@code type},
     * accumulating into a {@code long}.
     * <p>
     * Use this overload — modelled on {@link java.util.stream.Stream#mapToLong(ToLongFunction) Stream.mapToLong}
     * — when the payload exposes a primitive {@code long} field: it accepts a method reference like
     * {@code MyEvent::quantity} directly, without forcing the call site to wrap each value in
     * {@link BigDecimal#valueOf(long)}. For monetary and other domains where lossless decimal arithmetic
     * matters, prefer {@link #sum(Class, Function) sum(...)} over {@link BigDecimal}.
     *
     * @param type   the event payload type to sum over
     * @param mapper the projection from each event to a {@code long} addend
     * @param <E>    the event payload type
     * @return a {@link NumericCondition} producing the total as a {@link Long}
     */
    <E> NumericCondition<Long> sumLong(Class<E> type, ToLongFunction<? super E> mapper);

    /**
     * Returns the most recent event in the stream that is an instance of {@code type}, if any.
     *
     * @param type the event payload type to find
     * @param <E>  the event payload type
     * @return an {@link OptionalCondition} producing the last matching event
     */
    <E> OptionalCondition<E> latest(Class<E> type);

    /**
     * Returns the most recent event in the stream that matches any of the given types, expressed as an
     * {@link EventCondition} so it can be queried by type without unwrapping.
     *
     * @param types the candidate event payload types
     * @return an {@link EventCondition} producing the last matching event
     */
    EventCondition latestOf(Class<?>... types);

    /**
     * Starts a {@link MatchBuilder} flow mapping the most recent event in the stream onto a sealed result type
     * {@code R}. Equivalent to {@link #latestOf} followed by {@link EventCondition#matching}, but constrains the
     * candidate event types to those registered via {@link MatchBuilder#when}.
     *
     * @param resultType the type of the matched result, typically a sealed interface representing a domain status
     * @param <R>        the result type
     * @return a builder accumulating {@link MatchBuilder#when} clauses
     */
    <R> MatchBuilder<R> latestMatch(Class<R> resultType);

    /**
     * Returns the first event in the stream that is an instance of {@code type}, if any.
     *
     * @param type the event payload type to find
     * @param <E>  the event payload type
     * @return an {@link OptionalCondition} producing the first matching event
     */
    <E> OptionalCondition<E> first(Class<E> type);

    /**
     * Returns the first event in the stream that matches any of the given types, expressed as an
     * {@link EventCondition} so it can be queried by type without unwrapping.
     *
     * @param types the candidate event payload types
     * @return an {@link EventCondition} producing the first matching event
     */
    EventCondition firstOf(Class<?>... types);

    // ----------------------------------------------------------------------
    // Eager resolveXxx(...) shortcuts
    // ----------------------------------------------------------------------

    /**
     * Eager shortcut for {@link #contains(Class)}: builds the membership condition and forces it immediately,
     * returning a primitive {@code boolean}.
     * <p>
     * Equivalent to {@code contains(type).resolve()}. This forces a coordinated read of the scope at the point of
     * call (no batching with other conditions); use it when a single membership answer is wanted inline rather
     * than declared as a deferred {@link BooleanCondition}.
     *
     * @param type the event payload type to look for
     * @return {@code true} if any event in the stream is an instance of {@code type}
     */
    default boolean resolveContains(Class<?> type) {
        return contains(type).resolve();
    }

    /**
     * Eager shortcut for {@link #count(Class[])}: builds the count condition and forces it immediately, returning
     * a primitive {@code long}.
     * <p>
     * Equivalent to {@code count(types).resolve()}. This forces a coordinated read of the scope at the point of
     * call (no batching with other conditions); use it when a single count is wanted inline rather than declared
     * as a deferred {@link NumericCondition}.
     *
     * @param types the event payload types to count; at least one required
     * @return the combined count of events across all matching {@code types}
     */
    default long resolveCount(Class<?>... types) {
        return count(types).resolve();
    }

    /**
     * Eager shortcut for {@link #sum(Class, Function)}: builds the sum condition and forces it immediately,
     * returning the total as a {@link BigDecimal}.
     * <p>
     * Equivalent to {@code sum(type, mapper).resolve()}. This forces a coordinated read of the scope at the point
     * of call (no batching with other conditions); use it when a single total is wanted inline rather than
     * declared as a deferred {@link NumericCondition}.
     *
     * @param type   the event payload type to sum over
     * @param mapper the projection from each event to a {@link BigDecimal} addend
     * @param <E>    the event payload type
     * @return the total of {@code mapper} applied to every matching event
     */
    default <E> BigDecimal resolveSum(Class<E> type, Function<? super E, BigDecimal> mapper) {
        return sum(type, mapper).resolve();
    }

    /**
     * Eager shortcut for {@link #latest(Class)}: builds the selection condition and forces it immediately,
     * returning the most recent matching event as an {@link Optional}.
     * <p>
     * Equivalent to {@code latest(type).resolve()}. This forces a coordinated read of the scope at the point of
     * call (no batching with other conditions); use it when the latest event is wanted inline rather than
     * declared as a deferred {@link OptionalCondition}.
     *
     * @param type the event payload type to find
     * @param <E>  the event payload type
     * @return the most recent event of {@code type}, or {@link Optional#empty()} if none is present
     */
    default <E> Optional<E> resolveLatest(Class<E> type) {
        return latest(type).resolve();
    }

    /**
     * Eager shortcut for {@link #latestOf(Class[])}: builds the selection condition and forces it immediately,
     * returning the most recent matching event payload directly (or {@code null} if none matched), so it can be
     * consumed with {@code instanceof} at the call site.
     * <p>
     * Equivalent to {@code latestOf(types).resolve().orElse(null)}. This forces a coordinated read of the scope at
     * the point of call (no batching with other conditions); use it when the latest event among several types is
     * wanted inline for pattern matching rather than declared as a deferred {@link EventCondition}.
     *
     * @param types the candidate event payload types
     * @return the most recent event matching any of {@code types}, or {@code null} if none is present
     */
    default @Nullable Object resolveLatestOf(Class<?>... types) {
        return latestOf(types).resolve().orElse(null);
    }

    /**
     * Eager shortcut for {@link #first(Class)}: builds the selection condition and forces it immediately,
     * returning the earliest matching event as an {@link Optional}.
     * <p>
     * Equivalent to {@code first(type).resolve()}. This forces a coordinated read of the scope at the point of
     * call (no batching with other conditions); use it when the first event is wanted inline rather than declared
     * as a deferred {@link OptionalCondition}.
     *
     * @param type the event payload type to find
     * @param <E>  the event payload type
     * @return the earliest event of {@code type}, or {@link Optional#empty()} if none is present
     */
    default <E> Optional<E> resolveFirst(Class<E> type) {
        return first(type).resolve();
    }

}
