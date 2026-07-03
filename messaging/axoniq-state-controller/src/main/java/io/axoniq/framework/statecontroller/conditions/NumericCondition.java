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
import java.util.concurrent.CompletableFuture;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * A {@link Condition} whose value is a numeric quantity of type {@code N}, with comparison and arithmetic operators.
 * <p>
 * Comparisons produce a {@link BooleanCondition} so they slot directly into decision predicates without
 * intermediate boxing or re-wrapping. Arithmetic combinators ({@link #plus(Condition)}, {@link #minus(Condition)})
 * preserve the numeric type so derived totals remain fluent. The type parameter {@code N} is bounded to
 * {@link Comparable} so that the comparison operators can be defined uniformly across numeric domains
 * ({@link java.math.BigDecimal BigDecimal}, {@link Long}, {@link java.time.Instant Instant}, etc.).
 * <p>
 * Implementers describe the numeric domain through three atomic operations — {@link #zero()},
 * {@link #add(Comparable, Comparable)}, and {@link #subtract(Comparable, Comparable)} — plus
 * {@link Condition#resolveAsync()} on the base ({@link Condition#map(Function) map} and
 * {@link Condition#combine(Condition, java.util.function.BiFunction) zip} are defaulted). Every comparison and
 * arithmetic operator on this interface is derived from those primitives. Lift an arbitrary {@code Condition<N>}
 * into a {@code NumericCondition} via
 * {@link #of(Condition, java.util.function.BinaryOperator, java.util.function.BinaryOperator, Comparable) of(...)}.
 *
 * @param <N> the numeric value type
 * @author Allard Buijze
 * @since 5.2.0
 */
public interface NumericCondition<N extends Comparable<N>> extends Condition<N> {

    /**
     * Returns the additive identity (zero) of the numeric domain {@code N}.
     * <p>
     * Together with {@link #add(Comparable, Comparable) add} and {@link #subtract(Comparable, Comparable) subtract},
     * this describes the arithmetic of {@code N} that the interface's combinators are derived from. Used by
     * {@link #isPositive()} and {@link #isZero()} as the comparison reference; the underlying value itself is
     * still produced by {@link #value()}.
     *
     * @return the zero value for {@code N}
     */
    N zero();

    /**
     * Returns the sum of two values of {@code N}. Implementations must respect the additive identity
     * {@link #zero()}.
     * <p>
     * This is the arithmetic primitive backing {@link #plus(Condition) plus(...)}: the operator that combines
     * two numeric conditions into one. The interface needs the operation explicitly because Java's generics
     * cannot dispatch numeric arithmetic on {@code N} at runtime — every concrete numeric type
     * ({@link Long}, {@link java.math.BigDecimal BigDecimal}, …) provides its own addition.
     *
     * @param a the left-hand operand
     * @param b the right-hand operand
     * @return {@code a + b} for the domain
     */
    N add(N a, N b);

    /**
     * Returns the difference of two values of {@code N}.
     * <p>
     * This is the arithmetic primitive backing {@link #minus(Condition) minus(...)}. Like
     * {@link #add(Comparable, Comparable) add}, it is supplied explicitly because the interface has no other
     * way to invoke type-specific subtraction on the generic {@code N}.
     *
     * @param a the minuend
     * @param b the subtrahend
     * @return {@code a - b} for the domain
     */
    N subtract(N a, N b);

    /**
     * Returns a condition that is {@code true} when this value is strictly greater than {@code other}.
     *
     * @param other the value to compare against
     * @return a condition representing {@code this.value() > other}
     */
    default BooleanCondition isGreaterThan(N other) {
        return BooleanCondition.of(map(v -> v.compareTo(other) > 0));
    }

    /**
     * Returns a condition that is {@code true} when this value is greater than or equal to {@code other}.
     *
     * @param other the value to compare against
     * @return a condition representing {@code this.value() >= other}
     */
    default BooleanCondition isAtLeast(N other) {
        return BooleanCondition.of(map(v -> v.compareTo(other) >= 0));
    }

    /**
     * Returns a condition that is {@code true} when this value is strictly less than {@code other}.
     *
     * @param other the value to compare against
     * @return a condition representing {@code this.value() < other}
     */
    default BooleanCondition isLessThan(N other) {
        return BooleanCondition.of(map(v -> v.compareTo(other) < 0));
    }

    /**
     * Returns a condition that is {@code true} when this value is less than or equal to {@code other}.
     *
     * @param other the value to compare against
     * @return a condition representing {@code this.value() <= other}
     */
    default BooleanCondition isAtMost(N other) {
        return BooleanCondition.of(map(v -> v.compareTo(other) <= 0));
    }

    /**
     * Returns a condition that is {@code true} when this value is equal to {@code other}.
     *
     * @param other the value to compare against
     * @return a condition representing {@code this.value() == other}
     */
    default BooleanCondition isEqualTo(N other) {
        return BooleanCondition.of(map(v -> v.compareTo(other) == 0));
    }

    /**
     * Returns a condition that is {@code true} when this value is strictly greater than {@link #zero()}.
     *
     * @return a condition representing {@code this.value() > zero}
     */
    default BooleanCondition isPositive() {
        return isGreaterThan(zero());
    }

    /**
     * Returns a condition that is {@code true} when this value equals {@link #zero()}.
     *
     * @return a condition representing {@code this.value() == zero}
     */
    default BooleanCondition isZero() {
        return isEqualTo(zero());
    }

    /**
     * Returns the arithmetic sum of this condition's value and {@code other}'s value, preserving the numeric
     * domain through the resulting condition.
     *
     * @param other the right-hand operand
     * @return a condition representing {@code this.value() + other.value()}
     */
    default NumericCondition<N> plus(Condition<N> other) {
        return NumericCondition.of(combine(other, this::add), this::add, this::subtract, zero());
    }

    /**
     * Returns the arithmetic difference of this condition's value and {@code other}'s value, preserving the
     * numeric domain through the resulting condition.
     *
     * @param other the right-hand operand
     * @return a condition representing {@code this.value() - other.value()}
     */
    default NumericCondition<N> minus(Condition<N> other) {
        return NumericCondition.of(combine(other, this::subtract), this::add, this::subtract, zero());
    }

    /**
     * Lifts a {@link Condition} producing an {@code N} into a {@code NumericCondition} with explicit addition,
     * subtraction, and zero. If {@code base} is already a {@code NumericCondition}, it is returned as-is and the
     * supplied operators are ignored.
     *
     * @param base     the underlying condition producing values of {@code N}
     * @param add      the addition operator for values of {@code N}
     * @param subtract the subtraction operator for values of {@code N}
     * @param zero     the additive identity for {@code N}
     * @param <N>      the numeric value type
     * @return a numeric condition over {@code N}
     */
    static <N extends Comparable<N>> NumericCondition<N> of(Condition<N> base,
                                                            BinaryOperator<N> add,
                                                            BinaryOperator<N> subtract,
                                                            N zero) {
        if (base instanceof NumericCondition) {
            return (NumericCondition<N>) base;
        }
        return new NumericConditionImpl<>(base, add, subtract, zero);
    }

    /**
     * Thin delegating implementation used by
     * {@link #of(Condition, BinaryOperator, BinaryOperator, Comparable) of(...)} to wrap a {@code Condition<N>}
     * with numeric semantics. Internal; consumers should never reference this type directly.
     *
     * @param base     the underlying condition this implementation delegates to
     * @param addOp    the addition operator for {@code N}
     * @param subOp    the subtraction operator for {@code N}
     * @param zeroVal  the additive identity for {@code N}
     * @param <N>      the numeric value type
     */
    @Internal
    record NumericConditionImpl<N extends Comparable<N>>(Condition<N> base,
                                                         BinaryOperator<N> addOp,
                                                         BinaryOperator<N> subOp,
                                                         N zeroVal) implements NumericCondition<N> {

        /**
         * Compact constructor validating non-null arguments.
         */
        public NumericConditionImpl {
            Objects.requireNonNull(base, "base must not be null");
            Objects.requireNonNull(addOp, "addOp must not be null");
            Objects.requireNonNull(subOp, "subOp must not be null");
            Objects.requireNonNull(zeroVal, "zeroVal must not be null");
        }

        @Override
        public CompletableFuture<N> resolveAsync() {
            return base.resolveAsync();
        }

        @Override
        public N zero() {
            return zeroVal;
        }

        @Override
        public N add(N a, N b) {
            return addOp.apply(a, b);
        }

        @Override
        public N subtract(N a, N b) {
            return subOp.apply(a, b);
        }
    }
}
