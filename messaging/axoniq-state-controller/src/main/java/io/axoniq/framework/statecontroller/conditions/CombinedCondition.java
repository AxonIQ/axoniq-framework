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
import java.util.function.BiFunction;

/**
 * Decorator {@link Condition} that combines two upstream conditions' values through a {@link BiFunction}.
 * <p>
 * Its {@link #resolveAsync()} returns
 * {@code left.resolveAsync().thenCombine(right.resolveAsync(), combiner)} — both upstream futures
 * are awaited together, and this decorator never observes events or registers any accumulator on the backing
 * stream. Because conditions on the same scope share a single sourced read, both upstream futures complete in
 * lock-step, so {@code thenCombine} resolves as soon as the reduce finishes.
 * <p>
 * Marked {@link Internal @Internal} because instances are produced by
 * {@link Condition#zip(Condition, BiFunction)}; direct construction would skip the integration with the
 * surrounding {@link Condition} interface.
 *
 * @param <T> the left upstream value type
 * @param <U> the right upstream value type
 * @param <R> the combined value type
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
final class CombinedCondition<T, U, R> implements Condition<R> {

    private final Condition<T> left;
    private final Condition<U> right;
    private final BiFunction<? super T, ? super U, ? extends R> combiner;

    CombinedCondition(Condition<T> left,
                    Condition<U> right,
                    BiFunction<? super T, ? super U, ? extends R> combiner) {
        this.left = Objects.requireNonNull(left, "left must not be null");
        this.right = Objects.requireNonNull(right, "right must not be null");
        this.combiner = Objects.requireNonNull(combiner, "combiner must not be null");
    }

    @Override
    public CompletableFuture<R> resolveAsync() {
        return left.resolveAsync().thenCombine(right.resolveAsync(), combiner);
    }
}
