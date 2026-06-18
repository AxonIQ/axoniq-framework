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
import java.util.function.Function;

/**
 * A {@link Condition} whose value is a boolean, with logical combinators and readable evaluation methods.
 * <p>
 * {@link #resolve()} is the idiomatic way to consume a {@code BooleanCondition} in a decision body: it forces
 * the condition once and returns a {@link Boolean} that auto-unboxes to a primitive {@code boolean} for direct
 * use in an {@code if} statement (for example {@code if (closed.resolve())}); {@link #isFalse()} forces it and
 * returns the negated primitive. The logical combinators ({@link #and(BooleanCondition)},
 * {@link #or(BooleanCondition)}, {@link #not()}, {@link #xor(BooleanCondition)}) preserve the
 * {@code BooleanCondition} type so chains stay fluent without lifting back from {@link Condition}.
 * <p>
 * Implementers need only supply {@link Condition#asCompletableFuture()}; every other operation — including
 * {@link #map(Function) map} and {@link Condition#zip(Condition, java.util.function.BiFunction) zip} — is
 * derived from that primitive. Lift an arbitrary {@code Condition<Boolean>} into a {@code BooleanCondition} via
 * {@link #of(Condition)}.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
public interface BooleanCondition extends Condition<Boolean> {

    /**
     * Lifts a {@link Condition} producing a {@link Boolean} into a {@code BooleanCondition}. If {@code base} is
     * already a {@code BooleanCondition}, it is returned as-is.
     *
     * @param base the underlying boolean-producing condition
     * @return a {@code BooleanCondition} backed by {@code base}
     */
    static BooleanCondition of(Condition<Boolean> base) {
        return (base instanceof BooleanCondition bc) ? bc : new BooleanConditionImpl(base);
    }

    /**
     * Forces this condition and returns whether its value is {@code true}.
     * <p>
     * On a {@code BooleanCondition} the force verb is the inherited {@link #resolve()}, which returns a
     * {@link Boolean} that auto-unboxes to a primitive {@code boolean} for direct use in an {@code if} statement
     * (for example {@code if (closed.resolve())}). This method is retained only as a deprecated alias.
     *
     * @return {@code true} if and only if the condition's value is {@link Boolean#TRUE}
     * @deprecated in favour of {@link #resolve()}, the force verb on {@code Condition}; this method now delegates
     * to {@link #resolve()}
     */
    @Deprecated
    default boolean isTrue() {
        return Boolean.TRUE.equals(resolve());
    }

    /**
     * Forces this condition and returns whether its value is {@code false}.
     *
     * @return {@code true} if and only if the condition's value is {@link Boolean#FALSE}
     */
    default boolean isFalse() {
        return Boolean.FALSE.equals(resolve());
    }

    /**
     * Returns a condition equivalent to the logical conjunction of this and {@code other}.
     *
     * @param other the right-hand operand
     * @return a condition that is {@code true} when both operands are {@code true}
     */
    default BooleanCondition and(BooleanCondition other) {
        return BooleanCondition.of(zip(other, (a, b) -> a && b));
    }

    /**
     * Returns a condition equivalent to the logical disjunction of this and {@code other}.
     *
     * @param other the right-hand operand
     * @return a condition that is {@code true} when at least one operand is {@code true}
     */
    default BooleanCondition or(BooleanCondition other) {
        return BooleanCondition.of(zip(other, (a, b) -> a || b));
    }

    /**
     * Returns a condition equivalent to the logical negation of this condition.
     *
     * @return a condition that is {@code true} when this one is {@code false}, and vice versa
     */
    default BooleanCondition not() {
        return BooleanCondition.of(map(b -> !b));
    }

    /**
     * Returns a condition equivalent to the exclusive-or of this and {@code other}.
     *
     * @param other the right-hand operand
     * @return a condition that is {@code true} when exactly one operand is {@code true}
     */
    default BooleanCondition xor(BooleanCondition other) {
        return BooleanCondition.of(zip(other, (a, b) -> a ^ b));
    }

    /**
     * Thin delegating implementation used by {@link #of(Condition)} to wrap an arbitrary {@code Condition<Boolean>}
     * as a {@code BooleanCondition}. Internal; consumers should never reference this type directly.
     *
     * @param base the underlying condition this implementation delegates to
     */
    @Internal
    record BooleanConditionImpl(Condition<Boolean> base) implements BooleanCondition {

        /**
         * Compact constructor validating non-null arguments.
         */
        public BooleanConditionImpl {
            Objects.requireNonNull(base, "base must not be null");
        }

        @Override
        public CompletableFuture<Boolean> asCompletableFuture() {
            return base.asCompletableFuture();
        }
    }
}
