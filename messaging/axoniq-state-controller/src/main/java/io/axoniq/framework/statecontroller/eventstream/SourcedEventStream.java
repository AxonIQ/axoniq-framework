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
import io.axoniq.framework.statecontroller.conditions.Condition;
import io.axoniq.framework.statecontroller.conditions.FutureCondition;
import io.axoniq.framework.statecontroller.conditions.MatchBuilder;
import io.axoniq.framework.statecontroller.conditions.NumericCondition;
import io.axoniq.framework.statecontroller.conditions.OptionalCondition;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * Concrete {@link EventStream} for a single scope, implemented as a streaming-accumulator host.
 * <p>
 * Each typed helper ({@link #contains}, {@link #count}, {@link #sum}, {@link #sumLong}, {@link #latest},
 * {@link #latestOf}, {@link #first}, {@link #firstOf}, {@link #latestMatch}, {@link #fold}) constructs a
 * {@link SourcedCondition} that registers itself with this stream as an accumulator. Forcing any of those
 * conditions seals the stream exactly once: it issues a single
 * {@link org.axonframework.eventsourcing.eventstore.EventStoreTransaction#source EventStoreTransaction.source}
 * request whose {@link SourcingCondition} narrows to the scope's tag set and the registered event
 * {@link QualifiedName QualifiedNames}, and then drives the resulting {@link MessageStream} through a single
 * pass that calls {@link SourcedCondition#accept accept(EventMessage)} on every registered accumulator. No events
 * are ever materialized into a list; each event is observed in turn, then released.
 * <p>
 * <h3>Type matching is metadata-only.</h3>
 * Every match in this implementation compares {@link EventMessage#type() event.type().qualifiedName()} against
 * a {@link QualifiedName} resolved once at {@link #registerType(Class)} time via the configured
 * {@link MessageTypeResolver}. Event payloads are never inspected by the matching logic, so events whose
 * payload arrives serialized (e.g. as {@code byte[]}) match correctly without forcing deserialization. Only
 * user-supplied mappers — the typed mapper on {@link #sum} / {@link #sumLong}, the per-event reducer on
 * {@link FoldableCondition#event}, the WHEN mapper on {@link MatchBuilder#when} — invoke
 * {@link EventMessage#payload() payload()}, and they do so only after the type has already been confirmed.
 * <p>
 * <h3>Threading.</h3>
 * Instances are <em>not</em> thread-safe. AF5's command dispatch pipeline guarantees a single
 * {@link ProcessingContext} is processed by a single thread for the duration of an in-flight command, and
 * {@code SourcedEventStream} relies on that contract: {@code resolvedTypes}, {@code accumulators}, and
 * {@code sealed} are mutated without synchronization. User code should never hand a
 * {@link io.axoniq.framework.statecontroller.decisions.DecisionContext DecisionContext} (or any stream produced
 * by it) to another thread.
 * <p>
 * Helpers requiring at least one event type ({@link #count}, {@link #containsAnyOf}, {@link #latestOf},
 * {@link #firstOf}) reject empty arrays with {@link IllegalArgumentException}.
 * <p>
 * Marked {@link Internal @Internal} because instances are minted by
 * {@link io.axoniq.framework.statecontroller.decisions.DecisionContext#scope DecisionContext.scope(...)};
 * direct instantiation would skip the same-scope caching that
 * {@link io.axoniq.framework.statecontroller.decisions.DecisionContext DecisionContext}'s implementation performs.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
public final class SourcedEventStream implements EventStream {

    private final EventStore eventStore;
    private final ProcessingContext processingContext;
    private final MessageTypeResolver typeResolver;
    private final @Nullable Converter converter;
    private final Set<Tag> scopeTags;

    private final Map<Class<?>, QualifiedName> resolvedTypes = new LinkedHashMap<>();
    private final Set<QualifiedName> additionalNames = new LinkedHashSet<>();
    private final List<SourcedCondition<?>> accumulators = new ArrayList<>();
    private boolean sealed;

    public SourcedEventStream(EventStore eventStore,
                           ProcessingContext processingContext,
                           MessageTypeResolver typeResolver,
                           Set<Tag> scopeTags) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.processingContext = Objects.requireNonNull(processingContext, "processingContext must not be null");
        this.typeResolver = Objects.requireNonNull(typeResolver, "typeResolver must not be null");
        this.converter = resolveConverter(processingContext);
        this.scopeTags = Set.copyOf(scopeTags);
    }

    /**
     * Resolves a {@link Converter} from the surrounding {@link ProcessingContext}, returning {@code null} if
     * none is registered. Prefers {@link GeneralConverter}, the canonical low-level converter registered by
     * {@code MessagingConfigurationDefaults}; looking up the bare {@link Converter} type alone is ambiguous
     * because both {@code MessageConverter} and {@code EventConverter} also implement it.
     * <p>
     * {@code null} is a valid input to {@link EventMessage#payloadAs(Class, Converter) payloadAs} when the
     * payload is already an instance of the requested type — typical for in-memory test fixtures — so the
     * absence of an explicit converter is not a hard failure here.
     */
    private static @Nullable Converter resolveConverter(ProcessingContext context) {
        try {
            return context.component(GeneralConverter.class);
        } catch (ComponentNotFoundException notFound) {
            return null;
        }
    }

    // ----------------------------------------------------------------------
    // EventStream surface
    // ----------------------------------------------------------------------

    @Override
    public <T> FoldableCondition<T> fold(T initial) {
        return new SourcedFoldableCondition<>(this, initial);
    }

    @Override
    public BooleanCondition contains(Class<?> type) {
        Objects.requireNonNull(type, "type must not be null");
        QualifiedName name = registerType(type);
        return new ContainsAnyCondition(this, Set.of(name));
    }

    @Override
    public BooleanCondition containsAnyOf(Class<?>... types) {
        Set<QualifiedName> names = registerAllTypes(requireTypes(types));
        return new ContainsAnyCondition(this, names);
    }

    @Override
    public NumericCondition<Long> count(Class<?>... types) {
        Set<QualifiedName> names = registerAllTypes(requireTypes(types));
        return new CountCondition(this, names);
    }

    @Override
    public <E> NumericCondition<BigDecimal> sum(Class<E> type, Function<? super E, BigDecimal> mapper) {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(mapper, "mapper must not be null");
        QualifiedName name = registerType(type);
        return new BigDecimalSumCondition<>(this, name, type, mapper);
    }

    @Override
    public <E> NumericCondition<Long> sumLong(Class<E> type, ToLongFunction<? super E> mapper) {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(mapper, "mapper must not be null");
        QualifiedName name = registerType(type);
        return new LongSumCondition<>(this, name, type, mapper);
    }

    @Override
    public <E> OptionalCondition<E> latest(Class<E> type) {
        Objects.requireNonNull(type, "type must not be null");
        QualifiedName name = registerType(type);
        return typedPayloadOf(SourcedEventSelection.latestOf(this, Set.of(name)), type);
    }

    @Override
    public EventCondition latestOf(Class<?>... types) {
        Set<QualifiedName> names = registerAllTypes(requireTypes(types));
        return SourcedEventCondition.latestOf(this, names);
    }

    @Override
    public <R> MatchBuilder<R> latestMatch(Class<R> resultType) {
        Objects.requireNonNull(resultType, "resultType must not be null");
        return SourcedMatchBuilder.latestOfRegisteredTypes(this);
    }

    @Override
    public <E> OptionalCondition<E> first(Class<E> type) {
        Objects.requireNonNull(type, "type must not be null");
        QualifiedName name = registerType(type);
        return typedPayloadOf(SourcedEventSelection.firstOf(this, Set.of(name)), type);
    }

    @Override
    public EventCondition firstOf(Class<?>... types) {
        Set<QualifiedName> names = registerAllTypes(requireTypes(types));
        return SourcedEventCondition.firstOf(this, names);
    }

    // ----------------------------------------------------------------------
    // Package-private hooks for the lazy condition types
    // ----------------------------------------------------------------------

    /**
     * Registers an event payload class with this scope and returns its resolved {@link QualifiedName}.
     * Resolution goes through the configured {@link MessageTypeResolver}; the result is cached, so multiple
     * registrations for the same {@link Class} resolve once.
     */
    QualifiedName registerType(Class<?> payloadType) {
        Objects.requireNonNull(payloadType, "payloadType must not be null");
        if (sealed) {
            throw new LateConditionException("event type '" + payloadType.getName() + "' for scope " + scopeTags);
        }
        return resolvedTypes.computeIfAbsent(payloadType,
                                             c -> typeResolver.resolveOrThrow(c).qualifiedName());
    }

    /**
     * Registers an event type by {@link QualifiedName} (the cross-language path).
     */
    void registerType(QualifiedName name) {
        Objects.requireNonNull(name, "name must not be null");
        if (sealed) {
            throw new LateConditionException("event type '" + name + "' for scope " + scopeTags);
        }
        additionalNames.add(name);
    }

    /**
     * Resolves the given {@code type} to its {@link QualifiedName} via the configured
     * {@link MessageTypeResolver}, without registering it on the stream. Reuses an existing registration when
     * one is present. Used by post-seal projections such as
     * {@link io.axoniq.framework.statecontroller.eventstream.EventCondition#isA EventCondition.isA(Class)} that
     * need only to compare a single already-selected event's type, never to broaden the sourced read.
     */
    QualifiedName resolveType(Class<?> type) {
        Objects.requireNonNull(type, "type must not be null");
        QualifiedName known = resolvedTypes.get(type);
        return known != null ? known : typeResolver.resolveOrThrow(type).qualifiedName();
    }

    /**
     * Returns the {@link Converter} resolved from the surrounding {@link ProcessingContext}, or {@code null} if
     * none is registered. Used by the lazy condition types to call
     * {@link EventMessage#payloadAs(Class, Converter) payloadAs} for typed payload extraction; the
     * {@code payloadAs} contract allows a {@code null} converter when the payload is already an instance of the
     * requested type.
     */
    @Nullable Converter converter() {
        return converter;
    }

    /**
     * Registers a {@link SourcedCondition} as an accumulator on this stream. Throws
     * {@link LateConditionException} if the stream has already been sealed.
     */
    void register(SourcedCondition<?> accumulator) {
        if (sealed) {
            throw new LateConditionException("condition on scope " + scopeTags);
        }
        accumulators.add(accumulator);
    }

    /**
     * Seals the stream (idempotent) and <strong>kicks off</strong> the sourced read on the surrounding
     * {@link ProcessingContext}'s
     * {@link org.axonframework.eventsourcing.eventstore.EventStoreTransaction EventStoreTransaction}, returning
     * immediately. The reduction over the resulting {@link MessageStream} runs asynchronously through
     * {@link MessageStream#reduce(Object, java.util.function.BiFunction) reduce}; when it completes (normally
     * or exceptionally), every registered {@link SourcedCondition} is informed via
     * {@link SourcedCondition#complete()} or {@link SourcedCondition#completeExceptionally(Throwable)} so its
     * value-future resolves.
     * <p>
     * Each {@link SourcedCondition} exposes its own future through
     * {@link Condition#asCompletableFuture() asCompletableFuture()}; callers block on that per-condition future
     * when they need a synchronous value rather than on any central stream-level future. This split lets
     * multiple scopes on the same decision overlap their loads — once {@code seal()} runs on each scope, the
     * sourced reads execute in parallel, and a subsequent {@code value()} only blocks on the future for its
     * specific scope.
     */
    void seal() {
        if (sealed) {
            return;
        }
        sealed = true;
        if (accumulators.isEmpty()) {
            return;
        }
        if (resolvedTypes.isEmpty() && additionalNames.isEmpty()) {
            for (SourcedCondition<?> a : accumulators) {
                a.complete();
            }
            return;
        }
        SourcingCondition condition = buildSourcingCondition();
        MessageStream<? extends EventMessage> stream =
                eventStore.transaction(processingContext).source(condition);
        stream.reduce(this, (host, entry) -> {
                  EventMessage event = entry.message();
                  for (SourcedCondition<?> a : host.accumulators) {
                      a.accept(event);
                  }
                  return host;
              })
              .whenComplete((__, error) -> {
                  if (error != null) {
                      for (SourcedCondition<?> a : accumulators) {
                          a.completeExceptionally(error);
                      }
                  } else {
                      for (SourcedCondition<?> a : accumulators) {
                          a.complete();
                      }
                  }
              });
    }

    // ----------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------

    private SourcingCondition buildSourcingCondition() {
        Set<QualifiedName> allTypes = new LinkedHashSet<>(resolvedTypes.size() + additionalNames.size());
        allTypes.addAll(resolvedTypes.values());
        allTypes.addAll(additionalNames);
        var tagged = EventCriteria.havingTags(scopeTags);
        EventCriteria criteria = allTypes.isEmpty()
                ? tagged
                : tagged.andBeingOneOfTypes(allTypes.toArray(new QualifiedName[0]));
        return SourcingCondition.conditionFor(criteria);
    }

    private static Class<?>[] requireTypes(Class<?>[] types) {
        Objects.requireNonNull(types, "types must not be null");
        if (types.length == 0) {
            throw new IllegalArgumentException("at least one event type is required");
        }
        return types.clone();
    }

    private Set<QualifiedName> registerAllTypes(Class<?>[] types) {
        Set<QualifiedName> names = new HashSet<>(types.length);
        for (Class<?> t : types) {
            names.add(registerType(Objects.requireNonNull(t, "type must not be null")));
        }
        return names;
    }

    /**
     * Lifts a {@link SourcedEventSelection} (whose value-future carries the selected {@link EventMessage}) into an
     * {@link OptionalCondition} projecting that selection to its typed payload. Used by {@link #latest(Class)}
     * and {@link #first(Class)} so they share the same "select then thenApply" pattern that
     * {@link SourcedEventCondition} uses for the multi-type projections — keeping the source-side accumulator
     * payload-free until the user actually asks for the value.
     */
    private <E> OptionalCondition<E> typedPayloadOf(SourcedEventSelection selection, Class<E> type) {
        return OptionalCondition.of(new FutureCondition<>(
                selection.asCompletableFuture()
                         .thenApply(opt -> opt.map(em -> em.payloadAs(type, converter)))));
    }
}
