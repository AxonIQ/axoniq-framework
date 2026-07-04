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
 * Concrete {@link EventStream} for a single scope, implemented as a streaming-accumulator host. A scope is one
 * or more {@link ScopeBranch branches}: simple scopes have exactly one, and union scopes (built with the
 * {@code History} surface's {@code of(...).and(...).or(...)} chain) have several, OR-combined into one sourced
 * read whose per-branch criteria narrow to each branch's tags and — when the branch is
 * {@linkplain ScopeBranch#restricted() restricted} — its explicitly declared event types.
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
    private final List<ScopeBranch> branches;
    private final List<Set<QualifiedName>> branchNames;
    private final Set<QualifiedName> restrictedNames;
    private final boolean hasUnrestrictedBranch;
    private final Runnable resolveTrigger;

    private final Map<Class<?>, QualifiedName> resolvedTypes = new LinkedHashMap<>();
    private final Set<QualifiedName> conditionNames = new LinkedHashSet<>();
    private final List<SourcedCondition<?>> accumulators = new ArrayList<>();
    private boolean sealed;

    /**
     * Creates a stream for a simple, single-branch scope with no type restriction.
     *
     * @param eventStore        the event store to source from at seal time
     * @param processingContext the processing context the sourced read participates in
     * @param typeResolver      the resolver mapping registered payload classes to {@link QualifiedName}s
     * @param scopeTags         the tag set identifying this scope
     * @param resolveTrigger    invoked when any condition on this stream is resolved, before its future is
     *                          returned; the loading session passes its seal-all hook here so the first
     *                          resolution anywhere in a decision seals every declared scope at once
     */
    public SourcedEventStream(EventStore eventStore,
                              ProcessingContext processingContext,
                              MessageTypeResolver typeResolver,
                              Set<Tag> scopeTags,
                              Runnable resolveTrigger) {
        this(eventStore, processingContext, typeResolver,
             List.of(ScopeBranch.unrestricted(scopeTags)), resolveTrigger);
    }

    /**
     * Creates a stream over the given branches — a single branch for a simple scope, several for a union scope
     * built with {@code History.of(...).and(...).or(...)}.
     * <p>
     * Each {@linkplain ScopeBranch#restricted() restricted} branch's payload types are resolved to
     * {@link QualifiedName}s immediately; unrestricted branches accumulate the types registered by declared
     * conditions instead.
     *
     * @param eventStore        the event store to source from at seal time
     * @param processingContext the processing context the sourced read participates in
     * @param typeResolver      the resolver mapping payload classes to {@link QualifiedName}s
     * @param branches          the branches whose union this stream reads; at least one required
     * @param resolveTrigger    invoked when any condition on this stream is resolved; the loading session passes
     *                          its seal-all hook here so the first resolution anywhere in a decision seals every
     *                          declared scope at once
     */
    public SourcedEventStream(EventStore eventStore,
                              ProcessingContext processingContext,
                              MessageTypeResolver typeResolver,
                              List<ScopeBranch> branches,
                              Runnable resolveTrigger) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.processingContext = Objects.requireNonNull(processingContext, "processingContext must not be null");
        this.typeResolver = Objects.requireNonNull(typeResolver, "typeResolver must not be null");
        this.converter = resolveConverter(processingContext);
        Objects.requireNonNull(branches, "branches must not be null");
        if (branches.isEmpty()) {
            throw new IllegalArgumentException("at least one scope branch is required");
        }
        this.branches = List.copyOf(branches);
        this.branchNames = new ArrayList<>(this.branches.size());
        this.restrictedNames = new LinkedHashSet<>();
        boolean unrestricted = false;
        for (ScopeBranch branch : this.branches) {
            Set<QualifiedName> names = new LinkedHashSet<>(branch.payloadTypes().size());
            for (Class<?> type : branch.payloadTypes()) {
                names.add(resolvedTypes.computeIfAbsent(type,
                                                        c -> typeResolver.resolveOrThrow(c).qualifiedName()));
            }
            branchNames.add(names);
            restrictedNames.addAll(names);
            unrestricted |= !branch.restricted();
        }
        this.hasUnrestrictedBranch = unrestricted;
        this.resolveTrigger = Objects.requireNonNull(resolveTrigger, "resolveTrigger must not be null");
    }

    /**
     * Signals that a condition on this stream is being resolved. Runs the configured resolve trigger — the
     * loading session's seal-all hook — so every scope declared in the in-flight decision seals together, then
     * guarantees this stream itself is sealed (covering direct engine use without a session).
     */
    void triggerResolve() {
        resolveTrigger.run();
        seal();
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
     * <p>
     * A type already declared by a {@linkplain ScopeBranch#restricted() restricted} branch is resolved for
     * matching only — the branch declaration, not the condition, decides the criteria. Otherwise the type joins
     * the unrestricted branches' criteria; when every branch is restricted and none declares the type, the
     * sourced read could never contain it, so registration fails fast with {@link IllegalArgumentException}.
     */
    QualifiedName registerType(Class<?> payloadType) {
        Objects.requireNonNull(payloadType, "payloadType must not be null");
        if (sealed) {
            throw new LateConditionException(
                    "event type '" + payloadType.getName() + "' for scope " + describeScope());
        }
        QualifiedName name = resolvedTypes.computeIfAbsent(payloadType,
                                                           c -> typeResolver.resolveOrThrow(c).qualifiedName());
        noteConditionType(payloadType.getName(), name);
        return name;
    }

    /**
     * Registers an event type by {@link QualifiedName} (the cross-language path). Subject to the same
     * branch-coverage rules as {@link #registerType(Class)}.
     */
    void registerType(QualifiedName name) {
        Objects.requireNonNull(name, "name must not be null");
        if (sealed) {
            throw new LateConditionException("event type '" + name + "' for scope " + describeScope());
        }
        noteConditionType(name.toString(), name);
    }

    /**
     * Routes a condition-registered type into the criteria: covered by a restricted branch's declaration means
     * nothing to add; otherwise the type must have an unrestricted branch to land in, or the condition could
     * never observe a matching event.
     */
    private void noteConditionType(String typeDescription, QualifiedName name) {
        if (restrictedNames.contains(name)) {
            return;
        }
        if (!hasUnrestrictedBranch) {
            throw new IllegalArgumentException(
                    "Event type '" + typeDescription + "' is not declared by any branch of scope "
                            + describeScope() + ". A scope restricted with and(...) only reads the declared "
                            + "types; add the type to a branch, or drop the restriction.");
        }
        conditionNames.add(name);
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
            throw new LateConditionException("condition on scope " + describeScope());
        }
        accumulators.add(accumulator);
    }

    /**
     * Returns whether this stream has been sealed. Used by the runtime's loading session to decide whether a
     * scope lookup can still register declarations on this stream or must mint a fresh one (the
     * supplementary-read case).
     *
     * @return {@code true} when {@link #seal()} has run, {@code false} while declarations are still accepted
     */
    public boolean isSealed() {
        return sealed;
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
     * When the read actually sources (at least one accumulator and at least one registered type), the folded
     * {@link EventCriteria} is recorded in {@link ReadBoundaries} for the in-flight {@link ProcessingContext},
     * feeding the DCB coverage guard that runs before an accepted outcome's append.
     * <p>
     * Each {@link SourcedCondition} exposes its own future through
     * {@link Condition#resolveAsync() resolveAsync()}; callers block on that per-condition future
     * when they need a synchronous value rather than on any central stream-level future. This split lets
     * multiple scopes on the same decision overlap their loads — once {@code seal()} runs on each scope, the
     * sourced reads execute in parallel, and a subsequent {@code value()} only blocks on the future for its
     * specific scope.
     */
    public void seal() {
        if (sealed) {
            return;
        }
        sealed = true;
        if (accumulators.isEmpty()) {
            return;
        }
        if (conditionNames.isEmpty() && restrictedNames.isEmpty()) {
            for (SourcedCondition<?> a : accumulators) {
                a.complete();
            }
            return;
        }
        EventCriteria criteria = buildCriteria();
        ReadBoundaries.record(processingContext, criteria);
        MessageStream<? extends EventMessage> stream =
                eventStore.transaction(processingContext).source(SourcingCondition.conditionFor(criteria));
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

    /**
     * Folds the branches into one {@link EventCriteria}: each branch contributes its tags narrowed to either its
     * explicit type declaration (restricted branches) or the union of condition-registered types (unrestricted
     * branches); multiple branches are OR-combined so the union loads in a single sourced read.
     */
    private EventCriteria buildCriteria() {
        List<EventCriteria> parts = new ArrayList<>(branches.size());
        for (int i = 0; i < branches.size(); i++) {
            ScopeBranch branch = branches.get(i);
            Set<QualifiedName> names = branch.restricted() ? branchNames.get(i) : conditionNames;
            var tagged = EventCriteria.havingTags(branch.tags());
            parts.add(names.isEmpty()
                              ? tagged
                              : tagged.andBeingOneOfTypes(names.toArray(new QualifiedName[0])));
        }
        return parts.size() == 1 ? parts.getFirst() : EventCriteria.either(parts);
    }

    /**
     * Renders the scope for diagnostics: the tag set for a simple scope, the branch list (tags plus any type
     * restriction) for a union scope.
     */
    private String describeScope() {
        if (branches.size() == 1 && !branches.getFirst().restricted()) {
            return branches.getFirst().tags().toString();
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < branches.size(); i++) {
            ScopeBranch branch = branches.get(i);
            if (i > 0) {
                sb.append(" OR ");
            }
            sb.append(branch.tags());
            if (branch.restricted()) {
                sb.append(" of types ").append(branchNames.get(i));
            }
        }
        return sb.append(']').toString();
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
     * Lifts a {@link SourcedEventSelection} (whose value carries the selected {@link EventMessage}) into an
     * {@link OptionalCondition} projecting that selection to its typed payload. The projection is a lazy
     * {@link Condition#map(Function) map} — declaring {@link #latest(Class)} or {@link #first(Class)} performs
     * no resolution, keeping the source-side accumulator payload-free until the user actually asks for the
     * value.
     */
    private <E> OptionalCondition<E> typedPayloadOf(SourcedEventSelection selection, Class<E> type) {
        return OptionalCondition.of(selection.map(opt -> opt.map(em -> em.payloadAs(type, converter))));
    }
}
