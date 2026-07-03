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
import io.axoniq.framework.statecontroller.conditions.MatchBuilder;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Lazy {@link MatchBuilder} that accumulates per-type mappers and produces a {@link Condition} on
 * {@link #orDefault(Object)}.
 * <p>
 * Two construction modes coexist behind the same surface:
 * <ul>
 *     <li><strong>Pre-selected event mode</strong> — fed by
 *         {@link io.axoniq.framework.statecontroller.eventstream.EventCondition#matching EventCondition.matching}.
 *         The selection already narrows the loaded events to a single candidate (or empty); the builder applies
 *         whichever {@link #when(Class, Function) when(...)} mapper has a matching {@link QualifiedName} on the
 *         selected event. The terminal {@code orDefault(...)} returns a lazy {@code map} over the selection —
 *         no new accumulator is registered and nothing resolves at declaration time.</li>
 *     <li><strong>Latest-of-registered-types mode</strong> — fed by
 *         {@link io.axoniq.framework.statecontroller.eventstream.EventStream#latestMatch EventStream.latestMatch}.
 *         No pre-selection; on {@link #orDefault(Object) orDefault(...)} the builder creates a
 *         {@link SourcedCondition} that accumulates the latest event whose {@link QualifiedName} matches any
 *         registered {@code when} clause, mirroring the "constrains the candidate event types to those
 *         registered via MatchBuilder.when" contract on {@code EventStream.latestMatch}.</li>
 * </ul>
 * Each {@link #when(Class, Function)} call eagerly registers the event type with the backing
 * {@link SourcedEventStream} so the sourced read picks it up, and appends a mapper. The terminal
 * {@code orDefault(...)} creates the {@link Condition} that participates in the stream pass; intermediate
 * {@code MatchBuilder} instances do <em>not</em> register accumulators, so an unused build chain costs
 * nothing at seal time.
 * <p>
 * Matching is by {@link EventMessage#type() event.type().qualifiedName()} — never by
 * {@link EventMessage#payload() payload()}. The user-supplied {@code when} mapper is the only place that
 * touches the payload, and only after the matching type has been confirmed.
 * <p>
 * Marked {@link Internal @Internal} because the eager type-registration on every {@link #when} call relies on
 * construction going through one of the two documented entry points; direct instantiation would skip that
 * contract.
 *
 * @param <R> the result type produced by the matcher
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
final class SourcedMatchBuilder<R> implements MatchBuilder<R> {

    private final SourcedEventStream stream;
    private final @Nullable SourcedEventSelection selection;
    private final List<TypedMapper<R>> mappers;

    private SourcedMatchBuilder(SourcedEventStream stream,
                             @Nullable SourcedEventSelection selection,
                             List<TypedMapper<R>> mappers) {
        this.stream = Objects.requireNonNull(stream, "stream must not be null");
        this.selection = selection;
        this.mappers = mappers;
    }

    /**
     * Constructs a builder in "pre-selected event" mode. The {@code selection} is the upstream source whose
     * {@link SourcedEventSelection#resolveAsync() value-future} already narrows the loaded events to a
     * single candidate; the terminal {@code orDefault(...)} materializes the future at that moment (which is
     * the point that seals the stream) and chains {@code thenApply} to apply whichever mapper matches. Holding
     * the {@link SourcedEventSelection} reference rather than its future keeps {@link #when when(...)} calls free
     * to register additional event types after {@code matching(...)} has been called.
     */
    static <R> SourcedMatchBuilder<R> overSelectedMessage(SourcedEventStream stream,
                                                       SourcedEventSelection selection) {
        return new SourcedMatchBuilder<>(stream,
                                      Objects.requireNonNull(selection, "selection must not be null"),
                                      new ArrayList<>());
    }

    /**
     * Constructs a builder in "latest-of-registered-types" mode. The selection happens inside the
     * {@link SourcedCondition} returned by {@link #orDefault(Object)}: it accumulates the latest loaded event
     * whose {@link QualifiedName} matches any registered {@code when} clause.
     */
    static <R> SourcedMatchBuilder<R> latestOfRegisteredTypes(SourcedEventStream stream) {
        return new SourcedMatchBuilder<>(stream, null, new ArrayList<>());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Matching uses {@link EventMessage#type() event.type().qualifiedName()} against the resolved
     * {@link QualifiedName} for {@code type}; the mapper is only invoked after a confirmed type match. Payload
     * extraction goes through {@link EventMessage#payloadAs(Class, org.axonframework.conversion.Converter)
     * payloadAs}, so serialized payloads (e.g. {@code byte[]}) are deserialized via the configured
     * {@link org.axonframework.conversion.Converter Converter} only after the matching type has been confirmed.
     */
    @Override
    public <E> MatchBuilder<R> when(Class<E> type, Function<? super E, ? extends R> mapper) {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(mapper, "mapper must not be null");
        QualifiedName name = stream.registerType(type);
        var next = new ArrayList<>(mappers);
        next.add(new TypedMapper<>(name, event -> mapper.apply(event.payloadAs(type, stream.converter()))));
        return new SourcedMatchBuilder<>(stream, selection, next);
    }

    @Override
    public Condition<R> orDefault(R fallback) {
        List<TypedMapper<R>> snapshot = List.copyOf(mappers);
        if (selection != null) {
            // Lazy projection: declaring the match must not resolve the selection (and seal the scope).
            return selection.map(opt -> applyMappers(opt, snapshot, fallback));
        }
        return new LatestMatchCondition<>(stream, snapshot, fallback);
    }

    /**
     * Applies the first matching mapper to {@code selection}, falling back to {@code fallback} when the
     * selection is empty or no mapper matches the selected event's {@link QualifiedName}. Payload extraction
     * is delegated to the mapper, which calls {@code payloadAs} internally.
     */
    private static <R> R applyMappers(Optional<EventMessage> selection,
                                      List<TypedMapper<R>> mappers,
                                      R fallback) {
        if (selection.isEmpty()) {
            return fallback;
        }
        EventMessage event = selection.get();
        QualifiedName qn = event.type().qualifiedName();
        for (TypedMapper<R> m : mappers) {
            if (m.name().equals(qn)) {
                return m.map().apply(event);
            }
        }
        return fallback;
    }

    /**
     * Accumulator that observes events and tracks the latest one whose {@link QualifiedName} matches any of
     * the registered mappers. Used by the {@code EventStream.latestMatch} construction path.
     */
    private static final class LatestMatchCondition<R> extends SourcedCondition<R> {

        private final Set<QualifiedName> wantedNames;
        private final List<TypedMapper<R>> mappers;
        private final R fallback;
        private @Nullable EventMessage latest;

        LatestMatchCondition(SourcedEventStream stream, List<TypedMapper<R>> mappers, R fallback) {
            super(stream);
            this.mappers = mappers;
            this.fallback = fallback;
            this.wantedNames = collectNames(mappers);
        }

        private static <R> Set<QualifiedName> collectNames(List<TypedMapper<R>> mappers) {
            Set<QualifiedName> names = new HashSet<>(mappers.size());
            for (TypedMapper<R> m : mappers) {
                names.add(m.name());
            }
            return Set.copyOf(names);
        }

        @Override
        void accept(EventMessage event) {
            if (wantedNames.contains(event.type().qualifiedName())) {
                latest = event;
            }
        }

        @Override
        protected R finalValue() {
            return applyMappers(Optional.ofNullable(latest), mappers, fallback);
        }
    }

    /**
     * Per-clause typed mapper: matches events by {@link QualifiedName} and maps the type-confirmed
     * {@link EventMessage} to {@code R}. The {@link Function} is the only place where payload extraction
     * happens, and does so via
     * {@link EventMessage#payloadAs(Class, org.axonframework.conversion.Converter) payloadAs} so serialized
     * payloads route through the configured {@link org.axonframework.conversion.Converter Converter}.
     */
    private record TypedMapper<R>(QualifiedName name, Function<EventMessage, R> map) {

        TypedMapper {
            Objects.requireNonNull(name, "name must not be null");
            Objects.requireNonNull(map, "map must not be null");
        }
    }
}
