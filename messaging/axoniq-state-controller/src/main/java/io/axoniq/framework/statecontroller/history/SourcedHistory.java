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

package io.axoniq.framework.statecontroller.history;

import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.EventTypeRestrictableEventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Eager, narrowed {@link History} bound to an ordered list of DCB terms that fold into a single
 * {@link EventCriteria} on the first read.
 * <p>
 * A {@code SourcedHistory} is either <em>loaded</em> (read at least once, its criteria sealed) or still
 * <em>under construction</em> as a fluent builder. The builder operations {@link #of(Class[]) of(...)},
 * {@link #and(Class[]) and(...)}, {@link #or(String, Object) or(...)} and {@link #or(Class[]) or(...)} each return
 * a fresh, not-yet-loaded instance carrying an extended term list, leaving the receiver untouched; this keeps the
 * builder effectively immutable. A single term materializes to that term's criterion, multiple terms to
 * {@link EventCriteria#either(EventCriteria...) either(...)}. A term with tags only loads all event types for
 * those tags, a tagless term restricted to types matches those types across all tags, and a term with both
 * combines the two via {@link EventTypeRestrictableEventCriteria#andBeingOneOfTypes(MessageTypeResolver, Class[])
 * andBeingOneOfTypes(...)}. Class-to-type resolution uses the same {@link MessageTypeResolver} the membership reads
 * compare against, so the sourced criteria match what the reads expect.
 * <p>
 * On the first read, this history sources its scope's full slice once via
 * {@link EventStore#transaction(ProcessingContext) eventStore.transaction(ctx)}
 * {@link org.axonframework.eventsourcing.eventstore.EventStoreTransaction#source(SourcingCondition) .source(...)},
 * materializes the resulting {@link MessageStream} into a {@link List} of {@link EventMessage} via
 * {@link MessageStream#reduce(Object, java.util.function.BiFunction) reduce}, and answers every subsequent read
 * from that one in-memory snapshot. Sourcing on the active transaction lets
 * {@code DefaultEventStoreTransaction} capture and thread the DCB
 * {@link org.axonframework.eventsourcing.eventstore.ConsistencyMarker ConsistencyMarker} automatically; this
 * class never threads it manually.
 * <p>
 * <h3>Type matching is metadata-only.</h3>
 * Membership reads ({@link #has}, {@link #never}, {@link #lastWas}, {@link #count}) compare
 * {@link EventMessage#type() event.type().qualifiedName()} against the {@link QualifiedName} resolved once per
 * payload class via the configured {@link MessageTypeResolver}. Payloads are deserialized through
 * {@link EventMessage#payloadAs(java.lang.reflect.Type, Converter) payloadAs} only by the value-returning reads
 * ({@link #latest}, {@link #latestOf}, {@link #first}, {@link #total}, {@link #entry}), and only after the type
 * has already been confirmed by metadata.
 * <p>
 * <h3>Threading.</h3>
 * Instances are <em>not</em> thread-safe. AF5 guarantees a single {@link ProcessingContext} is processed by a
 * single thread for the duration of an in-flight command; this class relies on that contract, materializing its
 * snapshot without synchronization.
 * <p>
 * Marked {@link Internal @Internal} because instances are minted by {@link HistoryFactory}; user code only ever
 * sees the {@link History} interface.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
@Internal
public final class SourcedHistory implements History {

    /**
     * Safety-net timeout used when bridging the asynchronous sourced read back to the synchronous read API.
     * Thirty seconds is long enough that no realistic scope load should hit it under healthy conditions, but
     * short enough that pathological cases (deadlocks, partitioned event stores) surface as failures rather than
     * thread leaks.
     */
    private static final Duration HISTORY_LOAD_TIMEOUT = Duration.ofSeconds(30);

    private final EventStore eventStore;
    private final ProcessingContext processingContext;
    private final MessageTypeResolver typeResolver;
    private final @Nullable Converter converter;
    private final List<Term> terms;
    private final Map<Class<?>, QualifiedName> resolvedTypes = new HashMap<>();

    private @Nullable List<EventMessage> events;

    /**
     * Creates a {@code SourcedHistory} bound to {@code criteria}, sourcing events from {@code eventStore} as part
     * of {@code processingContext}. The criteria is treated as a single, opaque term, preserving the behaviour of
     * the {@link History#matching(EventCriteria) matching(...)} entry point.
     *
     * @param eventStore        the {@link EventStore} the scope's slice is sourced from
     * @param processingContext the {@link ProcessingContext} the sourced read participates in; its
     *                          {@link org.axonframework.eventsourcing.eventstore.ConsistencyMarker ConsistencyMarker}
     *                          is recorded automatically for the DCB append
     * @param typeResolver      the {@link MessageTypeResolver} mapping payload classes to {@link QualifiedName}s
     * @param converter         the {@link Converter} used to deserialize payloads, or {@code null} when payloads
     *                          are already instances of the requested type
     * @param criteria          the {@link EventCriteria} defining this narrowed scope
     */
    public SourcedHistory(EventStore eventStore,
                          ProcessingContext processingContext,
                          MessageTypeResolver typeResolver,
                          @Nullable Converter converter,
                          EventCriteria criteria) {
        this(eventStore,
             processingContext,
             typeResolver,
             converter,
             List.of(Term.ofCriteria(Objects.requireNonNull(criteria, "criteria must not be null"))));
    }

    private SourcedHistory(EventStore eventStore,
                           ProcessingContext processingContext,
                           MessageTypeResolver typeResolver,
                           @Nullable Converter converter,
                           List<Term> terms) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.processingContext = Objects.requireNonNull(processingContext, "processingContext must not be null");
        this.typeResolver = Objects.requireNonNull(typeResolver, "typeResolver must not be null");
        this.converter = converter;
        this.terms = terms;
    }

    /**
     * Mints a {@code SourcedHistory} builder whose first, tagless term is restricted to {@code types}, matching
     * events of those types across all tags. The returned instance is under construction; it is read-sealed only
     * on its first read. Used by the {@link RootHistory#of(Class[]) type-first builder entry point}.
     *
     * @param eventStore        the {@link EventStore} the scope's slice is sourced from
     * @param processingContext the {@link ProcessingContext} the sourced read participates in
     * @param typeResolver      the {@link MessageTypeResolver} mapping payload classes to {@link QualifiedName}s
     * @param converter         the {@link Converter} used to deserialize payloads, or {@code null} when payloads
     *                          are already instances of the requested type
     * @param types             the event payload classes the first, tagless term is restricted to
     * @return a {@code SourcedHistory} builder whose first term matches {@code types} across all tags
     * @throws IllegalArgumentException if {@code types} is empty
     */
    static SourcedHistory ofTypes(EventStore eventStore,
                                  ProcessingContext processingContext,
                                  MessageTypeResolver typeResolver,
                                  @Nullable Converter converter,
                                  Class<?>... types) {
        return new SourcedHistory(eventStore,
                                  processingContext,
                                  typeResolver,
                                  converter,
                                  List.of(Term.taglessTypes(requireTypes(types))));
    }

    @Override
    public History of(String tagKey, Object tagValue) {
        throw alreadyNarrowed();
    }

    @Override
    public History of(Class<?>... types) {
        throw alreadyNarrowed();
    }

    @Override
    public History matching(EventCriteria criteria) {
        throw alreadyNarrowed();
    }

    @Override
    public History and(Class<?>... types) {
        requireUnsealed();
        Class<?>[] requested = requireTypes(types);
        List<Term> next = new ArrayList<>(terms);
        Term current = next.remove(next.size() - 1);
        next.add(current.withAdditionalTypes(requested));
        return copyWithTerms(next);
    }

    @Override
    public History or(String tagKey, Object tagValue) {
        Objects.requireNonNull(tagKey, "tagKey must not be null");
        Objects.requireNonNull(tagValue, "tagValue must not be null");
        requireUnsealed();
        List<Term> next = new ArrayList<>(terms);
        next.add(Term.taggedScope(Tag.of(tagKey, tagValue.toString())));
        return copyWithTerms(next);
    }

    @Override
    public History or(Class<?>... types) {
        requireUnsealed();
        Class<?>[] requested = requireTypes(types);
        List<Term> next = new ArrayList<>(terms);
        next.add(Term.taglessTypes(requested));
        return copyWithTerms(next);
    }

    @Override
    public boolean has(Class<?> type) {
        Objects.requireNonNull(type, "type must not be null");
        QualifiedName name = resolve(type);
        for (EventMessage event : materialized()) {
            if (matches(event, name)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean never(Class<?> type) {
        return !has(type);
    }

    @Override
    public boolean lastWas(Class<?> type) {
        Objects.requireNonNull(type, "type must not be null");
        List<EventMessage> all = materialized();
        if (all.isEmpty()) {
            return false;
        }
        return matches(all.get(all.size() - 1), resolve(type));
    }

    @Override
    public <E> Optional<E> latest(Class<E> type) {
        Objects.requireNonNull(type, "type must not be null");
        QualifiedName name = resolve(type);
        EventMessage event = latestMatching(name);
        return event == null ? Optional.empty() : Optional.of(payload(event, type));
    }

    @Override
    public @Nullable Object latestOf(Class<?>... types) {
        Class<?>[] requested = requireTypes(types);
        List<EventMessage> all = materialized();
        for (ListIterator<EventMessage> it = all.listIterator(all.size()); it.hasPrevious(); ) {
            EventMessage event = it.previous();
            for (Class<?> type : requested) {
                if (matches(event, resolve(type))) {
                    return payload(event, type);
                }
            }
        }
        return null;
    }

    @Override
    public <E> Optional<E> first(Class<E> type) {
        Objects.requireNonNull(type, "type must not be null");
        QualifiedName name = resolve(type);
        for (EventMessage event : materialized()) {
            if (matches(event, name)) {
                return Optional.of(payload(event, type));
            }
        }
        return Optional.empty();
    }

    @Override
    public long count(Class<?>... types) {
        Class<?>[] requested = requireTypes(types);
        long matched = 0;
        for (EventMessage event : materialized()) {
            for (Class<?> type : requested) {
                if (matches(event, resolve(type))) {
                    matched++;
                    break;
                }
            }
        }
        return matched;
    }

    @Override
    public <E> BigDecimal total(Class<E> type, Function<? super E, BigDecimal> mapper) {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(mapper, "mapper must not be null");
        QualifiedName name = resolve(type);
        BigDecimal sum = BigDecimal.ZERO;
        for (EventMessage event : materialized()) {
            if (matches(event, name)) {
                BigDecimal value = mapper.apply(payload(event, type));
                if (value != null) {
                    sum = sum.add(value);
                }
            }
        }
        return sum;
    }

    @Override
    public <E> Optional<History.Entry<E>> entry(Class<E> type) {
        Objects.requireNonNull(type, "type must not be null");
        QualifiedName name = resolve(type);
        EventMessage event = latestMatching(name);
        if (event == null) {
            return Optional.empty();
        }
        return Optional.of(new History.Entry<>(event.timestamp(), payload(event, type)));
    }

    // ----------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------

    private List<EventMessage> materialized() {
        List<EventMessage> loaded = this.events;
        if (loaded != null) {
            return loaded;
        }
        SourcingCondition condition = SourcingCondition.conditionFor(buildCriteria());
        loaded = FutureUtils.joinAndUnwrap(
                eventStore.transaction(processingContext)
                          .source(condition)
                          .reduce(new ArrayList<EventMessage>(), (list, entry) -> {
                              list.add(entry.message());
                              return list;
                          }),
                HISTORY_LOAD_TIMEOUT);
        this.events = loaded;
        return loaded;
    }

    /**
     * Folds the accumulated {@link #terms} into a single {@link EventCriteria}: a single term yields that term's
     * criterion, multiple terms are combined with {@link EventCriteria#either(EventCriteria...) either(...)}.
     */
    private EventCriteria buildCriteria() {
        if (terms.size() == 1) {
            return criterionFor(terms.get(0));
        }
        List<EventCriteria> criteria = new ArrayList<>(terms.size());
        for (Term term : terms) {
            criteria.add(criterionFor(term));
        }
        return EventCriteria.either(criteria);
    }

    /**
     * Builds the {@link EventCriteria} for a single {@code term}: an opaque {@code matching(...)} criterion is used
     * verbatim; a tagless term selects the relevant types across all tags via
     * {@link EventCriteria#havingAnyTag() havingAnyTag()}; otherwise the term's tags are matched, optionally
     * restricted to its types. Class-to-type resolution goes through the configured {@link MessageTypeResolver} so
     * the sourced criteria align with the membership reads.
     */
    private EventCriteria criterionFor(Term term) {
        if (term.criteria() != null) {
            return term.criteria();
        }
        EventTypeRestrictableEventCriteria restrictable = term.tags().isEmpty()
                ? EventCriteria.havingAnyTag()
                : EventCriteria.havingTags(term.tags());
        if (term.types().isEmpty()) {
            return restrictable;
        }
        return restrictable.andBeingOneOfTypes(typeResolver, term.types().toArray(new Class<?>[0]));
    }

    /**
     * Returns a fresh, not-yet-loaded {@code SourcedHistory} carrying {@code nextTerms}, leaving this instance
     * untouched so the builder stays effectively immutable.
     */
    private SourcedHistory copyWithTerms(List<Term> nextTerms) {
        return new SourcedHistory(eventStore, processingContext, typeResolver, converter, nextTerms);
    }

    /**
     * Guards builder operations against mutation after the criteria has been sealed by the first read.
     */
    private void requireUnsealed() {
        if (this.events != null) {
            throw new IllegalStateException(
                    "history has already been read; its criteria is sealed and cannot be extended");
        }
    }

    private @Nullable EventMessage latestMatching(QualifiedName name) {
        List<EventMessage> all = materialized();
        for (ListIterator<EventMessage> it = all.listIterator(all.size()); it.hasPrevious(); ) {
            EventMessage event = it.previous();
            if (matches(event, name)) {
                return event;
            }
        }
        return null;
    }

    private QualifiedName resolve(Class<?> type) {
        return resolvedTypes.computeIfAbsent(type, c -> typeResolver.resolveOrThrow(c).qualifiedName());
    }

    private static boolean matches(EventMessage event, QualifiedName name) {
        return event.type().qualifiedName().equals(name);
    }

    private <E> E payload(EventMessage event, Class<E> type) {
        return event.payloadAs(type, converter);
    }

    private static Class<?>[] requireTypes(Class<?>[] types) {
        Objects.requireNonNull(types, "types must not be null");
        if (types.length == 0) {
            throw new IllegalArgumentException("at least one event type is required");
        }
        Class<?>[] copy = types.clone();
        for (Class<?> type : copy) {
            Objects.requireNonNull(type, "type must not be null");
        }
        return copy;
    }

    private static IllegalStateException alreadyNarrowed() {
        return new IllegalStateException("history is already narrowed; call of(...) or matching(...) only once");
    }

    /**
     * A single DCB term contributing one criterion to the folded {@link EventCriteria}.
     * <p>
     * A term is one of three shapes:
     * <ul>
     *     <li>an opaque {@code criteria} captured by {@link History#matching(EventCriteria) matching(...)} — then
     *     {@code tags} and {@code types} are unused;</li>
     *     <li>a tag-scoped term with a non-empty {@code tags} set, optionally restricted to {@code types};</li>
     *     <li>a tagless term with an empty {@code tags} set restricted to {@code types}, matching those types
     *     across all tags.</li>
     * </ul>
     * {@code types} is an insertion-ordered set so repeated classes collapse while the declared order is kept.
     *
     * @param criteria an opaque criterion to use verbatim, or {@code null} for a tag/type-driven term
     * @param tags     the tags the term matches (AND), empty for a tagless term
     * @param types    the event payload classes the term is restricted to, empty for no type restriction
     */
    private record Term(@Nullable EventCriteria criteria, Set<Tag> tags, Set<Class<?>> types) {

        private static Term ofCriteria(EventCriteria criteria) {
            return new Term(criteria, Set.of(), Set.of());
        }

        private static Term taggedScope(Tag tag) {
            return new Term(null, Set.of(tag), new LinkedHashSet<>());
        }

        private static Term taglessTypes(Class<?>[] types) {
            return new Term(null, Set.of(), new LinkedHashSet<>(List.of(types)));
        }

        private Term withAdditionalTypes(Class<?>[] additional) {
            Set<Class<?>> merged = new LinkedHashSet<>(types);
            merged.addAll(List.of(additional));
            return new Term(criteria, tags, merged);
        }
    }
}
