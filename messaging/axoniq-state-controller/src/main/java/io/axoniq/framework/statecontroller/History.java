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

package io.axoniq.framework.statecontroller;

import io.axoniq.framework.statecontroller.conditions.BooleanCondition;
import io.axoniq.framework.statecontroller.conditions.Condition;
import io.axoniq.framework.statecontroller.conditions.NumericCondition;
import io.axoniq.framework.statecontroller.conditions.OptionalCondition;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * The event history a state-controlled command handler reads its decision from.
 * <p>
 * A {@code History} is injected into a plain
 * {@link org.axonframework.messaging.commandhandling.annotation.CommandHandler @CommandHandler} method — the
 * {@code History} parameter together with an {@link Outcome} return type is what opts the handler into the State
 * Controller; there is no dedicated annotation. The injected root is <em>unbound</em>: narrow it to the scope(s)
 * the decision cares about with {@link #of(String, Object) of(tagKey, tagValue)} before reading. The same shape
 * serves one scope or many; a transfer simply narrows twice.
 * <p>
 * Every read declares a {@link Condition}: no I/O happens at the read site. The first
 * {@link Condition#resolve() resolve()} (or {@link Condition#resolveAsync() resolveAsync()}) seals all conditions
 * declared so far — across all scopes — into one event-store read, narrowed to exactly the tags and event types
 * those conditions touch. That precision matters twice over: it keeps the read small, and it <em>is</em> the
 * decision's Dynamic Consistency Boundary — an accepted outcome's events are appended conditionally on exactly
 * the slice of history that was read, no wider. Conditions declared after the first resolution are answered by a
 * supplementary read on the same transaction; correct, one extra round-trip. See {@link Condition} for the full
 * cost model.
 * <p>
 * Example:
 * <pre>{@code
 * @CommandHandler
 * public Outcome withdraw(Withdraw cmd, History history) {
 *     History account = history.of("account", cmd.accountId());
 *     var closed  = account.has(AccountClosed.class);
 *     var balance = account.total(MoneyDeposited.class, MoneyDeposited::amount)
 *                          .minus(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));
 *
 *     if (closed.resolve())                              return reject("account closed");
 *     if (balance.resolve().compareTo(cmd.amount()) < 0) return reject("insufficient funds");
 *     return accept(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
 * }
 * }</pre>
 * <p>
 * Reads on the unbound root are rejected with {@link IllegalStateException} — a decision must always name the
 * scope it reads, never query globally by accident.
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @since 5.2.0
 */
public interface History {

    /**
     * Narrows this history to the events tagged with the given key/value pair.
     * <p>
     * On the injected root, this selects a scope (for example {@code of("account", cmd.accountId())}). On an
     * already-narrowed history, it narrows further to events carrying <em>all</em> tags accumulated so far — the
     * composite-scope case, such as {@code of("course", courseId).of("student", studentId)}.
     *
     * @param tagKey   the tag key identifying the entity or slice this decision concerns
     * @param tagValue the tag value, typically an entity identifier; converted with {@link Object#toString()}
     * @return a history narrowed to the tagged slice
     */
    History of(String tagKey, Object tagValue);

    /**
     * Narrows this history to the events tagged with all of the given key/value pairs at once. Equivalent to
     * chaining {@link #of(String, Object)} per entry.
     *
     * @param tags the tag key/value pairs defining the scope; values are converted with {@link Object#toString()}
     * @return a history narrowed to the composite tagged slice
     */
    History of(Map<String, ?> tags);

    /**
     * Restricts the current branch of this history to the given event types.
     * <p>
     * Without a restriction, a branch's consistency boundary narrows to the union of the event types its declared
     * conditions read. An explicit restriction fixes the branch's type set instead: exactly these types are read
     * and guarded for the branch's tags, whether or not every one of them is touched by a condition. The
     * restriction is what makes a {@linkplain #or(String, Object) union scope} precise — when an event type could
     * carry either branch's tag, naming it on one branch says which slice of the union it belongs to, something no
     * after-the-fact filter can express against the store.
     * <p>
     * A condition declared on a restricted history may only read types that some branch declares (or that fall to
     * a branch without a restriction); reading an undeclared type fails fast with
     * {@link IllegalArgumentException}, because the sourced read could never contain it.
     * <p>
     * Repeated calls on the same branch accumulate types.
     *
     * @param types the event payload types the current branch reads; at least one required
     * @return a history whose current branch is restricted to the given types
     */
    History and(Class<?>... types);

    /**
     * Adds a branch to this history, turning it into (or extending) a <em>union scope</em>: one history spanning
     * multiple differently-tagged slices, read and guarded as a single consistency boundary.
     * <p>
     * The new branch starts scoped to the given key/value tag; subsequent {@link #of(String, Object) of(...)} and
     * {@link #and(Class[]) and(...)} calls narrow <em>that</em> branch. All branches load together in one sourced
     * read whose criteria are the union of each branch's tags × types:
     * <pre>{@code
     * History union = history.of("courseId", cmd.courseId())
     *                        .and(CourseCreated.class, CourseCapacityChanged.class,
     *                             StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class)
     *                        .or("studentId", cmd.studentId())
     *                        .and(StudentEnrolledInFaculty.class);
     * }</pre>
     * Conditions declared on the union observe the combined slice; every declared condition still resolves from
     * the same single read.
     *
     * @param tagKey   the tag key scoping the new branch
     * @param tagValue the tag value, typically an entity identifier; converted with {@link Object#toString()}
     * @return a history extended with a branch for the tagged slice
     */
    History or(String tagKey, Object tagValue);

    /**
     * Declares the condition that an event of the given {@code type} ever occurred in this scope.
     *
     * @param type the event payload type to look for
     * @return a condition answering "did this ever happen?"
     */
    BooleanCondition has(Class<?> type);

    /**
     * Declares the condition that no event of the given {@code type} ever occurred in this scope.
     *
     * @param type the event payload type to look for
     * @return a condition answering "did this never happen?"
     */
    BooleanCondition never(Class<?> type);

    /**
     * Declares a count of the events in this scope whose payload is an instance of any of the given {@code types}.
     *
     * @param types the event payload types to count; at least one required
     * @return a numeric condition producing the combined count across all matching types
     */
    NumericCondition<Long> count(Class<?>... types);

    /**
     * Declares the sum of the projection {@code amount} applied to every event in this scope of the given
     * {@code type}.
     *
     * @param type   the event payload type to sum over
     * @param amount the projection from each event to a {@link BigDecimal} addend
     * @param <E>    the event payload type
     * @return a numeric condition producing the total
     */
    <E> NumericCondition<BigDecimal> total(Class<E> type, Function<? super E, BigDecimal> amount);

    /**
     * Declares the sum of the projection {@code amount} applied to every event in this scope of the given
     * {@code type}, accumulating into a {@code long}.
     * <p>
     * Use this overload when the payload exposes a primitive {@code long} field. For monetary and other domains
     * where lossless decimal arithmetic matters, prefer {@link #total(Class, Function) total(...)} over
     * {@link BigDecimal}.
     *
     * @param type   the event payload type to sum over
     * @param amount the projection from each event to a {@code long} addend
     * @param <E>    the event payload type
     * @return a numeric condition producing the total as a {@link Long}
     */
    <E> NumericCondition<Long> totalLong(Class<E> type, ToLongFunction<? super E> amount);

    /**
     * Declares the most recent event in this scope of the given {@code type}, if any.
     *
     * @param type the event payload type to find
     * @param <E>  the event payload type
     * @return a condition producing the last matching event payload
     */
    <E> OptionalCondition<E> latest(Class<E> type);

    /**
     * Declares the most recent event in this scope among the given {@code types}, resolving to {@code null} when
     * none occurred. The nullable payload is designed for pattern matching:
     * <pre>{@code
     * if (bike.latestOf(RequestApproved.class, BikeReturned.class).resolve()
     *         instanceof RequestApproved approved) {
     *     ...
     * }
     * }</pre>
     * Naming only the types that matter is also what keeps the consistency boundary tight: types not listed are
     * neither read nor guarded.
     *
     * @param types the candidate event payload types; at least one required
     * @return a condition producing the last matching event payload, or {@code null} when none matched
     */
    Condition<@Nullable Object> latestOf(Class<?>... types);

    /**
     * Declares the first event in this scope of the given {@code type}, if any.
     *
     * @param type the event payload type to find
     * @param <E>  the event payload type
     * @return a condition producing the first matching event payload
     */
    <E> OptionalCondition<E> first(Class<E> type);
}
