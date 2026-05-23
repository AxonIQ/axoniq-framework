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

import io.axoniq.framework.statecontroller.conditions.Condition;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.function.BiFunction;

/**
 * A {@link Condition} whose value is produced by a multi-event-type fold, extended in place by attaching one
 * reducer per event type.
 * <p>
 * Returned by {@link EventStream#fold(Object) EventStream.fold(initial)} and by every chained
 * {@link #event event(...)} call. Each {@code event(...)} call registers its event type (class or qualified name)
 * with the surrounding loading-context so the {@code SourcingCondition} narrows the event-store query, and
 * appends a typed reducer that will be applied to matching events in chronological order. Reducers for different
 * event types compose into a single fold over the union of registered types; the accumulator threads through
 * every applicable reducer.
 * <p>
 * Because the result is both a {@code Condition} and the fold-builder shape, a fold can be forced, mapped,
 * zipped, or extended with another event type at any point in the chain — no separate terminator call is needed.
 * <p>
 * The class-based overload gives the reducer the deserialized payload directly. The {@link QualifiedName}-based
 * overload is the cross-language alternative for events whose payload class is not on the classpath; its reducer
 * receives the full {@link EventMessage}.
 *
 * @param <T> the accumulator type
 * @author Allard Buijze
 * @since 5.2.0
 */
public interface FoldableCondition<T> extends Condition<T> {

    /**
     * Registers a per-event-type reducer keyed on the given {@code type}, and returns the extended fold.
     *
     * @param type    the event payload type to include in the fold
     * @param reducer the function combining the accumulator with each matched event payload
     * @param <E>     the event payload type
     * @return the extended fold, ready to chain further {@link #event event(...)} calls or be forced
     */
    <E> FoldableCondition<T> event(Class<E> type, BiFunction<T, ? super E, T> reducer);

    /**
     * Registers a per-event-name reducer keyed on the given {@code name}, and returns the extended fold.
     * <p>
     * Use this overload when the event's Java payload class is not (or may not be) on the classpath — the
     * cross-language scenario. The reducer receives the full {@link EventMessage} so it can inspect the raw
     * payload, metadata, and timestamp without depending on a specific class.
     *
     * @param name    the qualified name identifying the event to include in the fold
     * @param reducer the function combining the accumulator with each matched event message
     * @return the extended fold, ready to chain further {@link #event event(...)} calls or be forced
     */
    FoldableCondition<T> event(QualifiedName name, BiFunction<T, EventMessage, T> reducer);
}
