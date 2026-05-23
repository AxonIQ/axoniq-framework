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
import io.axoniq.framework.statecontroller.conditions.MatchBuilder;
import io.axoniq.framework.statecontroller.conditions.OptionalCondition;

import java.util.Arrays;

/**
 * An {@link OptionalCondition} carrying a single event payload from an {@link EventStream}, with shape-aware
 * predicates for asking type-level questions.
 * <p>
 * Produced by {@link EventStream#latestOf(Class[])} and {@link EventStream#firstOf(Class[])} when a decision
 * body asks "what was the most recent (or earliest) event among these types?" without yet committing to a
 * specific result shape. The {@link #matching(Class)} operation branches into the {@link MatchBuilder} flow
 * when the next step is to map the matched event onto a sealed status type.
 * <p>
 * Unlike the other specialized conditions, {@code EventCondition} deliberately has no {@code of(...)} lifter.
 * Its identity is "an event whose type the framework registered with the loading-context"; constructing one
 * from an arbitrary {@code Condition<Optional<Object>>} would silently bypass that registration and produce
 * wrong answers for {@link #isA(Class)} and friends. Always obtain an {@code EventCondition} from
 * {@code EventStream} or from another {@code EventCondition}'s {@link #as(Class)} / {@link #matching(Class)}.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
public interface EventCondition extends OptionalCondition<Object> {

    /**
     * Returns a condition that is {@code true} when the underlying event is an instance of {@code type}.
     *
     * @param type the candidate event type
     * @return a {@link BooleanCondition} representing {@code value().filter(type::isInstance).isPresent()}
     */
    default BooleanCondition isA(Class<?> type) {
        return BooleanCondition.of(map(opt -> opt.filter(type::isInstance).isPresent()));
    }

    /**
     * Returns a condition that is {@code true} when the underlying event is an instance of any of the given types.
     *
     * @param types the candidate event types
     * @return a {@link BooleanCondition} representing membership in {@code types}
     */
    default BooleanCondition isAnyOf(Class<?>... types) {
        Class<?>[] copy = types.clone();
        return BooleanCondition.of(map(opt -> opt
                .filter(e -> Arrays.stream(copy).anyMatch(t -> t.isInstance(e)))
                .isPresent()));
    }

    /**
     * Returns a condition that is {@code true} when the underlying event was published under the given event name.
     * <p>
     * Useful for cross-language scenarios where two payload classes share a single logical event name. Requires
     * the framework's event-name resolver to be wired in.
     *
     * @param eventName the logical event name to match
     * @return a {@link BooleanCondition} matching by event name
     */
    BooleanCondition isNamed(String eventName);

    /**
     * Narrows the underlying optional event to the given concrete type. The resulting condition is empty when the
     * event is absent or not an instance of {@code type}.
     *
     * @param type the target event type
     * @param <E>  the target event type parameter
     * @return an {@link OptionalCondition} carrying the event downcast to {@code E}
     */
    default <E> OptionalCondition<E> as(Class<E> type) {
        return OptionalCondition.of(map(opt -> opt.filter(type::isInstance).map(type::cast)));
    }

    /**
     * Begins a {@link MatchBuilder} flow that maps the underlying event onto a sealed result type {@code R}.
     *
     * @param resultType the type of the matched result, typically a sealed interface representing a domain status
     * @param <R>        the result type
     * @return a builder accumulating {@link MatchBuilder#when} clauses
     */
    <R> MatchBuilder<R> matching(Class<R> resultType);
}
