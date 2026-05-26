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

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Lazy {@link FoldableCondition} that observes events as a streaming accumulator, folding each matched event
 * (in chronological order) through whichever reducer's {@link QualifiedName} matches the event's type.
 * <p>
 * Each {@link #event event(...)} call returns a <strong>new</strong> {@code SourcedFoldableCondition} carrying the
 * extended reducer chain — the previous instance keeps running its own (smaller) chain. For a fold chain of
 * length N, the stream accordingly carries N accumulators that all observe each delivered event, an
 * {@code O(N · M)} cost where M is the per-scope event count. In practice N is small (1–3 reducers per fold)
 * and the per-event work is a single hash-set lookup + a {@code BiFunction.apply}, so this is well within
 * budget for state-controller workloads.
 * <p>
 * Matching is by {@link EventMessage#type() event.type().qualifiedName()} — never by
 * {@link EventMessage#payload() payload()}. The user-supplied reducer is the only place payload is touched,
 * and only after the type has already been confirmed, so events whose payload arrives serialized
 * (e.g. as {@code byte[]}) are folded correctly without forcing deserialization for non-matching reducers.
 * <p>
 * Marked {@link Internal @Internal} because instances are produced by {@link SourcedEventStream#fold(Object)};
 * direct instantiation would skip the eager type-registration performed against the backing
 * {@link SourcedEventStream}.
 *
 * @param <T> the accumulator type
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
final class SourcedFoldableCondition<T> extends SourcedCondition<T> implements FoldableCondition<T> {

    private final T initial;
    private final List<Reducer<T>> reducers;
    private T accumulator;

    SourcedFoldableCondition(SourcedEventStream stream, T initial) {
        this(stream, initial, List.of());
    }

    private SourcedFoldableCondition(SourcedEventStream stream, T initial, List<Reducer<T>> reducers) {
        super(stream);
        this.initial = initial;
        this.reducers = reducers;
        this.accumulator = initial;
    }

    @Override
    void accept(EventMessage event) {
        QualifiedName qn = event.type().qualifiedName();
        for (Reducer<T> r : reducers) {
            if (r.name().equals(qn)) {
                accumulator = r.fn().apply(accumulator, event);
                return;
            }
        }
    }

    @Override
    protected T finalValue() {
        return accumulator;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Matching uses {@link EventMessage#type() event.type().qualifiedName()} against the resolved
     * {@link QualifiedName} for {@code type}, so events with a serialized payload (e.g. {@code byte[]}) still
     * match correctly. Once the type matches, the payload is extracted via
     * {@link EventMessage#payloadAs(Class, org.axonframework.conversion.Converter) payloadAs} so the configured
     * {@link org.axonframework.conversion.Converter Converter} drives any necessary deserialization.
     */
    @Override
    public <E> FoldableCondition<T> event(Class<E> type, BiFunction<T, ? super E, T> reducer) {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(reducer, "reducer must not be null");
        QualifiedName name = stream.registerType(type);
        return appendReducer(new Reducer<>(
                name,
                (acc, event) -> reducer.apply(acc, event.payloadAs(type, stream.converter()))));
    }

    @Override
    public FoldableCondition<T> event(QualifiedName name, BiFunction<T, EventMessage, T> reducer) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(reducer, "reducer must not be null");
        stream.registerType(name);
        return appendReducer(new Reducer<>(name, reducer));
    }

    private SourcedFoldableCondition<T> appendReducer(Reducer<T> reducer) {
        var next = new ArrayList<>(reducers);
        next.add(reducer);
        return new SourcedFoldableCondition<>(stream, initial, next);
    }

    /**
     * A single typed reducer in the fold chain: matches events by {@link QualifiedName} and folds the matching
     * event into the accumulator. The {@code fn} {@code BiFunction} is the only place where
     * {@link EventMessage#payload() payload()} is touched.
     */
    private record Reducer<T>(QualifiedName name,
                              BiFunction<T, EventMessage, T> fn) {

        Reducer {
            Objects.requireNonNull(name, "name must not be null");
            Objects.requireNonNull(fn, "fn must not be null");
        }
    }
}
