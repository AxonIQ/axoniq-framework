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

import io.axoniq.framework.statecontroller.conditions.BooleanCondition;
import io.axoniq.framework.statecontroller.conditions.FutureCondition;
import io.axoniq.framework.statecontroller.conditions.MatchBuilder;
import io.axoniq.framework.statecontroller.conditions.OptionalCondition;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/**
 * Lazy {@link EventCondition} that exposes the payload of a single candidate {@link EventMessage} selected by
 * a {@link SourcedEventSelection} source accumulator.
 * <p>
 * <h3>Composition, not inheritance</h3>
 * {@code SourcedEventCondition} does <em>not</em> itself extend {@link SourcedCondition}; the {@link SourcedEventSelection}
 * it wraps is the registering accumulator. All projection operators ({@link #isA(Class)}, {@link #isAnyOf(Class...)},
 * {@link #isNamed(String)}, {@link #as(Class)}, {@link #matching(Class)}) chain {@code thenApply} on the
 * selection's future and lift the result back into the {@link io.axoniq.framework.statecontroller.conditions.Condition
 * Condition} world via {@link FutureCondition}. Because the chain rides on the selection-future, no projection
 * needs its own seal/await handshake — the future itself encodes the await.
 * <p>
 * <h3>Metadata-only matching</h3>
 * {@link #isA(Class)}, {@link #isAnyOf(Class...)}, {@link #isNamed(String)} and {@link #as(Class)} compare
 * {@link EventMessage#type() event.type().qualifiedName()} directly — they never invoke
 * {@link EventMessage#payload() payload()} for the type check, so they work correctly against events whose
 * payload arrives serialized (e.g. as {@code byte[]}). {@link #as(Class)} matches by {@link QualifiedName} first
 * and only touches {@code payload()} on a confirmed match, which is when deserialization is genuinely required.
 * The default {@link #asCompletableFuture()} (returning {@code Optional<Object>}) likewise only invokes
 * {@link EventMessage#payload() payload()} on a present selection.
 * <p>
 * Marked {@link Internal @Internal} because instances are produced by
 * {@link SourcedEventStream#latestOf(Class[])} / {@link SourcedEventStream#firstOf(Class[])} (and from another
 * {@code EventCondition}'s {@link #matching(Class)} call); manual instantiation would skip the type
 * registration the surrounding {@link SourcedEventStream} performs.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
final class SourcedEventCondition implements EventCondition {

    private final SourcedEventStream stream;
    private final SourcedEventSelection selection;

    private SourcedEventCondition(SourcedEventStream stream, SourcedEventSelection selection) {
        this.stream = Objects.requireNonNull(stream, "stream must not be null");
        this.selection = Objects.requireNonNull(selection, "selection must not be null");
    }

    static SourcedEventCondition latestOf(SourcedEventStream stream, Set<QualifiedName> wantedNames) {
        return new SourcedEventCondition(stream, SourcedEventSelection.latestOf(stream, wantedNames));
    }

    static SourcedEventCondition firstOf(SourcedEventStream stream, Set<QualifiedName> wantedNames) {
        return new SourcedEventCondition(stream, SourcedEventSelection.firstOf(stream, wantedNames));
    }

    /**
     * Returns the {@link SourcedEventSelection}'s value-future — the selected {@link EventMessage} (or empty) once
     * the underlying load has completed. Used internally by projection operators so they can chain
     * {@code thenApply} on the selection without forcing payload extraction.
     */
    CompletableFuture<Optional<EventMessage>> selectionFuture() {
        return selection.asCompletableFuture();
    }

    @Override
    public CompletableFuture<Optional<Object>> asCompletableFuture() {
        // payload() is deferred to this terminal — and only invoked once the selection is non-empty.
        return selectionFuture().thenApply(opt -> opt.map(EventMessage::payload));
    }

    @Override
    public BooleanCondition isA(Class<?> type) {
        Objects.requireNonNull(type, "type must not be null");
        QualifiedName name = stream.resolveType(type);
        return booleanFromSelection(name::equals);
    }

    @Override
    public BooleanCondition isAnyOf(Class<?>... types) {
        Objects.requireNonNull(types, "types must not be null");
        if (types.length == 0) {
            throw new IllegalArgumentException("at least one event type is required");
        }
        Set<QualifiedName> names = new HashSet<>(types.length);
        for (Class<?> t : types) {
            names.add(stream.resolveType(Objects.requireNonNull(t, "type must not be null")));
        }
        Set<QualifiedName> snapshot = Set.copyOf(names);
        return booleanFromSelection(snapshot::contains);
    }

    @Override
    public BooleanCondition isNamed(String eventName) {
        Objects.requireNonNull(eventName, "eventName must not be null");
        return booleanFromSelection(qn -> qn.name().equals(eventName));
    }

    /**
     * Lifts a {@link QualifiedName}-level predicate over the selection-future into a {@link BooleanCondition}.
     * The predicate is evaluated against the selected event's qualified name when present; an empty selection
     * yields {@code false}. Used by {@link #isA}, {@link #isAnyOf}, and {@link #isNamed} to share a single
     * future-chaining shape.
     */
    private BooleanCondition booleanFromSelection(Predicate<QualifiedName> predicate) {
        return BooleanCondition.of(new FutureCondition<>(
                selectionFuture().thenApply(opt -> opt.map(em -> predicate.test(em.type().qualifiedName()))
                                                      .orElse(false))));
    }

    @Override
    public <E> OptionalCondition<E> as(Class<E> type) {
        Objects.requireNonNull(type, "type must not be null");
        QualifiedName name = stream.resolveType(type);
        return OptionalCondition.of(new FutureCondition<>(
                selectionFuture().thenApply(opt -> opt
                        .filter(em -> em.type().qualifiedName().equals(name))
                        // Match by QualifiedName first (no payload touch), then extract via payloadAs so a
                        // serialized payload is deserialized through the configured Converter only after the
                        // type has been confirmed.
                        .map(em -> em.payloadAs(type, stream.converter())))));
    }

    @Override
    public <R> MatchBuilder<R> matching(Class<R> resultType) {
        Objects.requireNonNull(resultType, "resultType must not be null");
        // Pass the SourcedEventSelection itself (not its future) so the builder can defer
        // asCompletableFuture() — and thus the stream's seal — until orDefault(...) is called,
        // leaving room for further when(...) calls to register additional event types.
        return SourcedMatchBuilder.overSelectedMessage(stream, selection);
    }
}
