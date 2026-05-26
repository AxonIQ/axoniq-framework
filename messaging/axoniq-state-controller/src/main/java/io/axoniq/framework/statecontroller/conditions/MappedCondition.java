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
 * Decorator {@link Condition} that lazily projects an upstream condition's value through a {@link Function}.
 * <p>
 * Its {@link #asCompletableFuture()} returns {@code source.asCompletableFuture().thenApply(fn)} — the upstream
 * future is the single source of truth, and this decorator never observes events or registers any accumulator
 * on the backing stream. Chained {@code .map(...)} calls fuse into a single {@code MappedCondition} so that
 * deep projection chains do not allocate intermediate condition objects per stage.
 * <p>
 * Marked {@link Internal @Internal} because instances are produced by {@link Condition#map(Function)} and
 * direct construction would skip that fusing guarantee.
 *
 * @param <T> the upstream value type
 * @param <U> the projected value type
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
final class MappedCondition<T, U> implements Condition<U> {

    private final Condition<T> source;
    private final Function<? super T, ? extends U> fn;

    MappedCondition(Condition<T> source, Function<? super T, ? extends U> fn) {
        this.source = Objects.requireNonNull(source, "source must not be null");
        this.fn = Objects.requireNonNull(fn, "fn must not be null");
    }

    @Override
    public CompletableFuture<U> asCompletableFuture() {
        return source.asCompletableFuture().thenApply(fn);
    }

    @Override
    public <V> Condition<V> map(Function<? super U, ? extends V> g) {
        Objects.requireNonNull(g, "g must not be null");
        return new MappedCondition<>(source, t -> g.apply(fn.apply(t)));
    }
}
