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

import java.util.function.Function;

/**
 * Fluent builder for mapping a single event onto a sealed result type {@code R}.
 * <p>
 * Each call to {@link #when(Class, Function)} registers a per-type mapping; {@link #orDefault(Object)} closes
 * the builder by supplying the value to use when none of the registered types matched. The terminal call
 * returns a {@link Condition} that, when forced, evaluates the underlying event in one pass and produces an
 * {@code R}. This is the building block behind the {@code latestMatch(...)} pattern on
 * {@code EventStream} (and behind {@code EventCondition.matching(...)}); it lets domain code project event
 * history onto sealed state types and {@code switch} on them in a decision body.
 *
 * @param <R> the result type produced by the matcher
 * @author Allard Buijze
 * @since 5.2.0
 */
public interface MatchBuilder<R> {

    /**
     * Registers a mapping from events of type {@code type} to results of type {@code R}.
     *
     * @param type   the event type to match against
     * @param mapper the function producing an {@code R} from a matched event
     * @param <E>    the event type
     * @return this builder, for chaining
     */
    <E> MatchBuilder<R> when(Class<E> type, Function<? super E, ? extends R> mapper);

    /**
     * Closes this builder with a fallback result for events that did not match any registered {@link #when}
     * clause, and returns the corresponding {@link Condition}.
     *
     * @param fallback the result to produce when no registered type matched
     * @return a condition producing the matched (or fallback) {@code R}
     */
    Condition<R> orDefault(R fallback);
}
