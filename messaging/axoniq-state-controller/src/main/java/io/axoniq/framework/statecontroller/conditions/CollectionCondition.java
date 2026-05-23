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

import org.axonframework.common.annotation.Internal;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A {@link Condition} whose value is a {@link List} of {@code T}, with shape-aware operators for asking shape
 * questions ({@link #isEmpty()}, {@link #size()}, {@link #contains(Object)}) and lightweight transformations
 * ({@link #mapEach(Function)}).
 * <p>
 * No method on {@code EventStream} currently produces a {@code CollectionCondition} — the type is here for users
 * who lift their own list-shaped conditions (or build domain helpers that return lists) without having to drop
 * back to the base {@link Condition} surface. The framework deliberately exposes no event-stream helper that
 * materializes events into a list, since that pattern invites unbounded loads.
 * <p>
 * All specialized operations are derived from {@link #map(Function)}; implementers only supply {@link #value()}
 * and {@link #map(Function)} on the base ({@link Condition#zip(Condition, java.util.function.BiFunction) zip}
 * is defaulted). Size is exposed as a {@link NumericCondition} of {@link Integer}, with addition derived from
 * {@link Integer#sum(int, int)} and zero from {@code 0}. Lift an arbitrary {@code Condition<List<T>>} into a
 * {@code CollectionCondition} via {@link #of(Condition)}.
 *
 * @param <T> the element type
 * @author Allard Buijze
 * @since 5.2.0
 */
public interface CollectionCondition<T> extends Condition<List<T>> {

    /**
     * Lifts a {@link Condition} producing a {@link List} into a {@code CollectionCondition}. If {@code base} is
     * already a {@code CollectionCondition}, it is returned as-is.
     *
     * @param base the underlying list-producing condition
     * @param <T>  the element type
     * @return a {@code CollectionCondition} backed by {@code base}
     */
    static <T> CollectionCondition<T> of(Condition<List<T>> base) {
        if (base instanceof CollectionCondition) {
            return (CollectionCondition<T>) base;
        }
        return new CollectionConditionImpl<>(base);
    }

    /**
     * Returns a condition that is {@code true} when the underlying list is empty.
     *
     * @return a {@link BooleanCondition} representing {@code value().isEmpty()}
     */
    default BooleanCondition isEmpty() {
        return BooleanCondition.of(map(List::isEmpty));
    }

    /**
     * Returns the size of the underlying list as a numeric condition.
     *
     * @return a {@link NumericCondition} representing {@code value().size()}
     */
    default NumericCondition<Integer> size() {
        return NumericCondition.of(map(List::size), Integer::sum, (a, b) -> a - b, 0);
    }

    /**
     * Returns a condition that is {@code true} when the underlying list contains {@code item}.
     *
     * @param item the candidate element
     * @return a {@link BooleanCondition} representing {@code value().contains(item)}
     */
    default BooleanCondition contains(T item) {
        return BooleanCondition.of(map(list -> list.contains(item)));
    }

    /**
     * Returns a condition over the list with each element transformed by {@code fn}.
     *
     * @param fn  the per-element transformation
     * @param <U> the target element type
     * @return a condition representing {@code value().stream().map(fn).toList()}
     */
    default <U> CollectionCondition<U> mapEach(Function<? super T, ? extends U> fn) {
        return CollectionCondition.of(map(list -> list.stream().<U>map(fn).toList()));
    }

    /**
     * Thin delegating implementation used by {@link #of(Condition)} to wrap an arbitrary
     * {@code Condition<List<T>>} as a {@code CollectionCondition}. Internal; consumers should never reference
     * this type directly.
     *
     * @param base the underlying condition this implementation delegates to
     * @param <T>  the element type
     */
    @Internal
    record CollectionConditionImpl<T>(Condition<List<T>> base) implements CollectionCondition<T> {

        /**
         * Compact constructor validating non-null arguments.
         */
        public CollectionConditionImpl {
            Objects.requireNonNull(base, "base must not be null");
        }

        @Override
        public List<T> value() {
            return base.value();
        }

        @Override
        public <U> Condition<U> map(Function<? super List<T>, ? extends U> fn) {
            return base.map(fn);
        }
    }
}
