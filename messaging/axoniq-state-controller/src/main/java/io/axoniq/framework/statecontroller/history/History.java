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

package io.axoniq.framework.statecontroller.history;

import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;

/**
 * A decision's window onto recorded events. Injected into a {@code @Decide} method as a single parameter, then
 * narrowed to the scope(s) the decision cares about with {@link #of(String, Object) of(tagKey, tagValue)} and read
 * with a small, business-readable vocabulary that always returns plain values.
 * <p>
 * The injected {@code History} is <em>unbound</em>: the read methods are only meaningful on the result of
 * {@link #of(String, Object) of(...)} (or the advanced {@link #matching(EventCriteria) matching(...)}), never on
 * the root parameter itself. Calling a read method on the unbound root is a programming error and fails fast.
 * <p>
 * Reads are eager and consistent: the first read of a narrowed {@code History} sources that scope's slice once and
 * records the DCB consistency marker for the in-flight command, and every later read on the same narrowed
 * {@code History} answers from that one snapshot. A decision therefore observes a single consistent view of its
 * scope.
 * <p>
 * Single scope:
 * <pre>{@code
 * @Decide
 * Decision withdraw(Withdraw cmd, History history) {
 *     History account = history.of("account", cmd.accountId());
 *     if (account.has(AccountClosed.class)) return reject("account closed");
 *
 *     BigDecimal balance = account.total(MoneyDeposited.class, MoneyDeposited::amount)
 *                          .subtract(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));
 *     if (balance.compareTo(cmd.amount()) < 0) return reject("insufficient funds");
 *
 *     return accept(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
 * }
 * }</pre>
 * <p>
 * Advanced scopes: when a decision must span several DCB terms, the fluent builder expresses multi-term criteria
 * without touching {@link EventCriteria} directly. Each {@link #of(String, Object) of(...)} /
 * {@link #of(Class[]) of(...)} starts a term, {@link #and(Class[]) and(...)} narrows the current term to event
 * types, and {@link #or(String, Object) or(...)} / {@link #or(Class[]) or(...)} begins a new term. On the first
 * read the terms are combined: one term yields that term's criterion, several are OR-ed with
 * {@link EventCriteria#either(EventCriteria...) either(...)}.
 * <pre>{@code
 * History scope = history.of("courseId", id.courseId())
 *                            .and(CourseCreated.class, CourseCapacityChanged.class,
 *                                 StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class)
 *                        .or("studentId", id.studentId())
 *                            .and(StudentEnrolledInFaculty.class,
 *                                 StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class);
 * }</pre>
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
public interface History {

    /**
     * Narrows this {@code History} to the scope identified by the tag {@code tagKey=tagValue}, returning a
     * scoped {@code History} ready to be read. A bare tag scope matches every event type carrying the tag; chain
     * {@link #and(Class[]) and(...)} to restrict it to specific event types (a tag-then-types term), narrowing the
     * consistency boundary accordingly.
     *
     * @param tagKey   the tag key identifying the scope (for example {@code "account"})
     * @param tagValue the tag value identifying the concrete entity within the scope; converted with
     *                 {@link Object#toString()}
     * @return a scoped {@code History} bound to events tagged {@code tagKey=tagValue}
     * @throws IllegalStateException if this {@code History} is already narrowed
     */
    History of(String tagKey, Object tagValue);

    /**
     * Narrows this {@code History} to the tag scope {@code tagKey=tagValue} and restricts it to the given event
     * {@code types} in one call — the type-precise single-scope form. Equivalent to
     * {@link #of(String, Object) of(tagKey, tagValue)}{@code .}{@link #and(Class[]) and(types)}, or to bare
     * {@code of(tagKey, tagValue)} when {@code types} is empty.
     * <p>
     * Prefer this over the bare {@link #of(String, Object)} when a decision reads only specific event types: the
     * consistency boundary then covers exactly those {@code (tag, types)} rather than every event sharing the tag,
     * narrowing the conflict surface and avoiding false conflicts with unrelated events.
     *
     * @param tagKey   the tag key identifying the scope (for example {@code "account"})
     * @param tagValue the tag value identifying the concrete entity within the scope; converted with
     *                 {@link Object#toString()}
     * @param types    the event payload classes the scope is restricted to; when empty, no type restriction is
     *                 applied (equivalent to {@link #of(String, Object)})
     * @return a scoped {@code History} bound to events tagged {@code tagKey=tagValue} and restricted to
     *         {@code types}
     * @throws IllegalStateException if this {@code History} is already narrowed
     */
    default History of(String tagKey, Object tagValue, Class<?>... types) {
        History scope = of(tagKey, tagValue);
        return types.length == 0 ? scope : scope.and(types);
    }

    /**
     * Starts a fluent {@link EventCriteria} builder whose first term is <em>tagless</em> and restricted to the
     * given event {@code types}, matching events of those types across all tags. Equivalent to a single DCB term
     * {@link EventCriteria#havingAnyTag() havingAnyTag()}{@code .andBeingOneOfTypes(types)}.
     * <p>
     * The returned {@code History} is a builder under construction: chain {@link #and(Class[]) and(...)} to widen
     * the current term, or {@link #or(String, Object) or(...)} / {@link #or(Class[]) or(...)} to begin a new term.
     * The accumulated terms are turned into a single {@link EventCriteria} on the first read — one term yields that
     * term's criterion, multiple terms are combined with {@link EventCriteria#either(EventCriteria...) either(...)}.
     * <p>
     * <strong>Boundary warning:</strong> a tagless term matches events of these types across the entire event
     * store. Used as the <em>sole</em> scope of an accepting decision it makes the append's consistency boundary
     * global — every concurrent append of these types, anywhere, conflicts. Prefer
     * {@link #of(String, Object, Class[]) of(tagKey, tagValue, types)} for an entity-scoped decision, or pair this
     * with a tagged {@link #or(String, Object) or(...)} term.
     *
     * @param types the event payload classes the first, tagless term is restricted to; at least one is required
     * @return a {@code History} builder whose first term matches the given {@code types} across all tags
     * @throws IllegalStateException    if this {@code History} is already narrowed
     * @throws IllegalArgumentException if {@code types} is empty
     */
    History of(Class<?>... types);

    /**
     * Narrows the current (most recently started) term of this builder to also be restricted to the given event
     * {@code types}, mirroring DCB {@code andBeingOneOfTypes(...)}. Combine with {@link #of(String, Object)} for a
     * tag-then-types term, or with {@link #or(String, Object)} / {@link #or(Class[])} to narrow a later term.
     * <p>
     * Returns a new builder; the receiver is left unchanged.
     *
     * @param types the event payload classes to add to the current term; at least one is required
     * @return a {@code History} builder whose current term is restricted to the given {@code types}
     * @throws IllegalStateException    if invoked on an unbound root (no term has been started with
     *                                  {@link #of(String, Object)} or {@link #of(Class[])}), or if this builder has
     *                                  already been read (its criteria is sealed once loaded)
     * @throws IllegalArgumentException if {@code types} is empty
     */
    History and(Class<?>... types);

    /**
     * Begins a new term scoped to the tag {@code tagKey=tagValue}, combined with the preceding terms using DCB
     * {@link EventCriteria#either(EventCriteria...) either(...)}. The new term carries no type restriction until
     * narrowed with {@link #and(Class[]) and(...)}.
     * <p>
     * Returns a new builder; the receiver is left unchanged.
     *
     * @param tagKey   the tag key identifying the new term's scope (for example {@code "studentId"})
     * @param tagValue the tag value identifying the concrete entity within the scope; converted with
     *                 {@link Object#toString()}
     * @return a {@code History} builder with a new term scoped to {@code tagKey=tagValue}
     * @throws IllegalStateException if invoked on an unbound root (no term has been started), or if this builder
     *                               has already been read (its criteria is sealed once loaded)
     */
    History or(String tagKey, Object tagValue);

    /**
     * Begins a new <em>tagless</em> term restricted to the given event {@code types}, combined with the preceding
     * terms using DCB {@link EventCriteria#either(EventCriteria...) either(...)}. The new term matches events of
     * those types across all tags.
     * <p>
     * <strong>Boundary warning:</strong> like {@link #of(Class[])}, a tagless term used as the <em>sole</em> scope
     * of an accepting decision makes the append's consistency boundary global; pair it with a tagged term.
     * <p>
     * Returns a new builder; the receiver is left unchanged.
     *
     * @param types the event payload classes the new, tagless term is restricted to; at least one is required
     * @return a {@code History} builder with a new tagless term restricted to {@code types}
     * @throws IllegalStateException    if invoked on an unbound root (no term has been started), or if this builder
     *                                  has already been read (its criteria is sealed once loaded)
     * @throws IllegalArgumentException if {@code types} is empty
     */
    History or(Class<?>... types);

    /**
     * Narrows this {@code History} to events matching the given {@code criteria}, returning a scoped
     * {@code History} ready to be read. The advanced, power-user entry point; most decisions use
     * {@link #of(String, Object)} instead.
     *
     * @param criteria the {@link EventCriteria} the scoped {@code History} sources events for
     * @return a scoped {@code History} bound to events matching {@code criteria}
     * @throws IllegalStateException if this {@code History} is already narrowed
     */
    History matching(EventCriteria criteria);

    /**
     * Returns whether an event of the given {@code type} ever occurred within this scope.
     *
     * @param type the event payload class to test for
     * @return {@code true} if at least one event of {@code type} occurred, {@code false} otherwise
     * @throws IllegalStateException if invoked on an unbound (not yet narrowed) {@code History}
     */
    boolean has(Class<?> type);

    /**
     * Returns whether no event of the given {@code type} ever occurred within this scope; the inverse of
     * {@link #has(Class)}.
     *
     * @param type the event payload class to test for
     * @return {@code true} if no event of {@code type} occurred, {@code false} otherwise
     * @throws IllegalStateException if invoked on an unbound (not yet narrowed) {@code History}
     */
    boolean never(Class<?> type);

    /**
     * Returns whether the single most recent event in this scope is of the given {@code type}. The boundary is
     * the scope's tag set: every event in the scope is considered, not only events of {@code type}.
     *
     * @param type the event payload class to test the newest event against
     * @return {@code true} if the newest event in the scope is of {@code type}, {@code false} if the scope is
     *         empty or its newest event is of another type
     * @throws IllegalStateException if invoked on an unbound (not yet narrowed) {@code History}
     */
    boolean lastWas(Class<?> type);

    /**
     * Returns the most recent event of the given {@code type} within this scope, if any.
     *
     * @param type the event payload class to select
     * @param <E>  the event payload type
     * @return an {@link Optional} carrying the latest event of {@code type}, or empty if none occurred
     * @throws IllegalStateException if invoked on an unbound (not yet narrowed) {@code History}
     */
    <E> Optional<E> latest(Class<E> type);

    /**
     * Returns the most recent event among the given {@code types} within this scope, or {@code null} if none
     * occurred. The bare deserialized payload is returned so callers can branch with {@code instanceof} or a
     * pattern {@code switch}.
     *
     * @param types the event payload classes to consider; at least one is required
     * @return the newest event whose type is among {@code types}, or {@code null} if none occurred
     * @throws IllegalStateException    if invoked on an unbound (not yet narrowed) {@code History}
     * @throws IllegalArgumentException if {@code types} is empty
     */
    @Nullable Object latestOf(Class<?>... types);

    /**
     * Returns the first (oldest) event of the given {@code type} within this scope, if any.
     *
     * @param type the event payload class to select
     * @param <E>  the event payload type
     * @return an {@link Optional} carrying the first event of {@code type}, or empty if none occurred
     * @throws IllegalStateException if invoked on an unbound (not yet narrowed) {@code History}
     */
    <E> Optional<E> first(Class<E> type);

    /**
     * Returns how many events of the given {@code types} occurred within this scope.
     *
     * @param types the event payload classes to count; at least one is required
     * @return the number of events in this scope whose type is among {@code types}
     * @throws IllegalStateException    if invoked on an unbound (not yet narrowed) {@code History}
     * @throws IllegalArgumentException if {@code types} is empty
     */
    long count(Class<?>... types);

    /**
     * Returns the sum of a {@link BigDecimal} field, extracted by {@code mapper}, over every event of the given
     * {@code type} within this scope. Yields {@link BigDecimal#ZERO} when no event of {@code type} occurred.
     *
     * @param type   the event payload class to sum over
     * @param mapper extracts the {@link BigDecimal} field to sum from each event of {@code type}
     * @param <E>    the event payload type
     * @return the sum of {@code mapper} applied to every event of {@code type}, or {@link BigDecimal#ZERO} if none
     * @throws IllegalStateException if invoked on an unbound (not yet narrowed) {@code History}
     */
    <E> BigDecimal total(Class<E> type, Function<? super E, BigDecimal> mapper);

    /**
     * Returns the most recent event of the given {@code type} within this scope together with its recorded
     * timestamp, if any. The rare time-aware accessor; most rules use the bare-payload {@link #latest(Class)}.
     *
     * @param type the event payload class to select
     * @param <E>  the event payload type
     * @return an {@link Optional} carrying the latest {@link Entry} of {@code type}, or empty if none occurred
     * @throws IllegalStateException if invoked on an unbound (not yet narrowed) {@code History}
     */
    <E> Optional<Entry<E>> entry(Class<E> type);

    /**
     * A selected event paired with the moment it was recorded.
     *
     * @param occurredAt the timestamp at which the event was recorded
     * @param payload    the deserialized event payload
     * @param <E>        the event payload type
     */
    record Entry<E>(Instant occurredAt, E payload) {
    }
}
