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

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A deferred question about a slice of event history that produces a single value of type {@code T} when forced.
 * <p>
 * Conditions are the unit of composition in a State Controller decision. They are declared procedurally but not
 * evaluated until {@link #value()} (or a specialized evaluator such as
 * {@link BooleanCondition#isTrue()}) is invoked. Declaring a condition records intent; evaluating it triggers a
 * single coordinated load of the underlying events for the enclosing scope. Holding a {@code Condition} reference
 * is therefore cheap and free of side effects.
 * <p>
 * Specialized subtypes ({@link BooleanCondition}, {@link NumericCondition}, {@link OptionalCondition},
 * {@link CollectionCondition}) add operations that only make sense for their shape and return the specialized
 * type so chains stay fluent without re-wrapping. Use {@link #map(Function)} and {@link #zip(Condition, BiFunction)}
 * when you need to combine values whose shape does not fit one of the specialized subtypes.
 *
 * @param <T> the type of value produced when this condition is forced
 * @author Allard Buijze
 * @since 5.2.0
 */
public interface Condition<T> {

    /**
     * Forces this condition, returning its value.
     * <p>
     * The first invocation of {@code value()} (directly or via a specialized evaluator) on any condition declared
     * in the same scope triggers a single load against the event store covering the union of all conditions
     * declared at that point. Subsequent invocations reuse the loaded events.
     *
     * @return the value produced by this condition
     */
    T value();

    /**
     * Returns a new condition that applies {@code fn} to this condition's value.
     *
     * @param fn  the transformation to apply when this condition is forced
     * @param <U> the target value type
     * @return a condition producing {@code fn(value())}
     */
    <U> Condition<U> map(Function<? super T, ? extends U> fn);

    /**
     * Combines this condition with {@code other} using {@code combiner}, producing a single condition over the
     * joined values. Both source conditions are evaluated together when the resulting condition is forced.
     * <p>
     * The default implementation expresses {@code zip} in terms of {@link #map(Function)} and {@code other.value()}.
     * Because every condition is declared before any is forced, {@code other} is already present in the shared
     * loading-context and its value is read from the unified load triggered by this condition's {@link #value()}.
     * Implementations may override for efficiency or to register additional structural dependencies up front.
     *
     * @param other    the other condition to combine with
     * @param combiner the function that joins both values into the result
     * @param <U>      the value type of {@code other}
     * @param <R>      the result value type
     * @return a condition producing {@code combiner(value(), other.value())}
     */
    default <U, R> Condition<R> zip(Condition<U> other, BiFunction<? super T, ? super U, ? extends R> combiner) {
        return map(t -> combiner.apply(t, other.value()));
    }
}
