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

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A {@link Condition} whose value is an {@link Optional}, with shape-aware operators that avoid the
 * {@code Optional}-of-{@code Condition} double wrapping.
 * <p>
 * {@link #isPresent()} and {@link #isAbsent()} project presence into a {@link BooleanCondition}.
 * {@link #mapPresent(Function)} threads a transformation through the optional without forcing it.
 * {@link #orDefault(Object)} unwraps to a plain {@link Condition} carrying the unconditional value.
 * <p>
 * All specialized operations are derived from {@link #map(Function)}; implementers only supply
 * {@link Condition#asCompletableFuture()} on the base ({@link Condition#map(Function) map} and
 * {@link Condition#zip(Condition, java.util.function.BiFunction) zip} are defaulted). Lift an arbitrary
 * {@code Condition<Optional<T>>} into an {@code OptionalCondition} via {@link #of(Condition)}.
 *
 * @param <T> the value type wrapped by the optional
 * @author Allard Buijze
 * @since 5.2.0
 */
public interface OptionalCondition<T> extends Condition<Optional<T>> {

    /**
     * Lifts a {@link Condition} producing an {@link Optional} into an {@code OptionalCondition}. If {@code base}
     * is already an {@code OptionalCondition}, it is returned as-is.
     *
     * @param base the underlying optional-producing condition
     * @param <T>  the value type wrapped by the optional
     * @return an {@code OptionalCondition} backed by {@code base}
     */
    static <T> OptionalCondition<T> of(Condition<Optional<T>> base) {
        if (base instanceof OptionalCondition) {
            return (OptionalCondition<T>) base;
        }
        return new OptionalConditionImpl<>(base);
    }

    /**
     * Returns a condition that is {@code true} when the underlying optional contains a value.
     *
     * @return a {@link BooleanCondition} representing {@code value().isPresent()}
     */
    default BooleanCondition isPresent() {
        return BooleanCondition.of(map(Optional::isPresent));
    }

    /**
     * Returns a condition that is {@code true} when the underlying optional is empty.
     *
     * @return a {@link BooleanCondition} representing {@code value().isEmpty()}
     */
    default BooleanCondition isAbsent() {
        return BooleanCondition.of(map(Optional::isEmpty));
    }

    /**
     * Returns a condition producing the contained value if present, otherwise {@code fallback}.
     *
     * @param fallback the value to use when the optional is empty
     * @return a condition representing {@code value().orElse(fallback)}
     */
    default Condition<T> orDefault(T fallback) {
        return map(opt -> opt.orElse(fallback));
    }

    /**
     * Returns a condition over the contained value transformed by {@code fn}, leaving an empty optional empty.
     *
     * @param fn  the transformation to apply when the optional carries a value
     * @param <U> the target value type
     * @return a condition representing {@code value().map(fn)}
     */
    default <U> OptionalCondition<U> mapPresent(Function<? super T, ? extends U> fn) {
        return OptionalCondition.of(map(opt -> opt.map(fn)));
    }

    /**
     * Thin delegating implementation used by {@link #of(Condition)} to wrap an arbitrary
     * {@code Condition<Optional<T>>} as an {@code OptionalCondition}. Internal; consumers should never reference
     * this type directly.
     *
     * @param base the underlying condition this implementation delegates to
     * @param <T>  the value type wrapped by the optional
     */
    @Internal
    record OptionalConditionImpl<T>(Condition<Optional<T>> base) implements OptionalCondition<T> {

        /**
         * Compact constructor validating non-null arguments.
         */
        public OptionalConditionImpl {
            Objects.requireNonNull(base, "base must not be null");
        }

        @Override
        public CompletableFuture<Optional<T>> asCompletableFuture() {
            return base.asCompletableFuture();
        }
    }
}
