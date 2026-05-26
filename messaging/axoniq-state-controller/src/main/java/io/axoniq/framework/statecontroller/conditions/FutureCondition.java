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

/**
 * Minimal {@link Condition} view over an already-existing {@link CompletableFuture}.
 * <p>
 * Used by source implementations to lift a derived future (for example, a {@code thenApply}-chained projection
 * over a registered accumulator's future) back into the {@code Condition} world without registering a new
 * accumulator on the backing stream. Combined with {@link Condition#map(java.util.function.Function) map(...)}
 * and the specialized {@code of(Condition)} lifters on
 * {@link BooleanCondition}, {@link OptionalCondition}, and {@link io.axoniq.framework.statecontroller.conditions.NumericCondition NumericCondition},
 * this is enough to express every "derived from an accumulator's selection" projection without per-call
 * supplier indirection.
 * <p>
 * Marked {@link Internal @Internal} because it is part of the sourced-condition wiring; outside that wiring there
 * is no general reason to wrap an arbitrary {@link CompletableFuture} as a {@link Condition}.
 *
 * @param <T> the value type carried by the underlying future
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
public final class FutureCondition<T> implements Condition<T> {

    private final CompletableFuture<T> future;

    /**
     * Creates a {@link FutureCondition} backed by the given {@code future}.
     *
     * @param future the future whose result this condition exposes
     */
    public FutureCondition(CompletableFuture<T> future) {
        this.future = Objects.requireNonNull(future, "future must not be null");
    }

    @Override
    public CompletableFuture<T> asCompletableFuture() {
        return future;
    }
}
