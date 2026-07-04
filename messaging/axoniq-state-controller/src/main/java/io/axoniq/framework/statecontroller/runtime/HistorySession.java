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

package io.axoniq.framework.statecontroller.runtime;

import io.axoniq.framework.statecontroller.History;
import io.axoniq.framework.statecontroller.decisions.DecisionContext;
import io.axoniq.framework.statecontroller.eventstream.EventStream;
import io.axoniq.framework.statecontroller.eventstream.ScopeBranch;
import io.axoniq.framework.statecontroller.eventstream.SourcedEventStream;
import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventstreaming.Tag;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The per-command loading session behind both decision surfaces: the {@link History} reads made by
 * state-controlled {@code @CommandHandler} methods and the lazy conditions declared through the
 * {@link DecisionContext} it implements.
 * <p>
 * Both surfaces funnel through {@link #scopeFor(Set)}, so a decision mixing them still batches: the session keeps
 * one {@link SourcedEventStream} per tag set, and {@link #sealAll()} — invoked by the first
 * {@link io.axoniq.framework.statecontroller.conditions.Condition#resolveAsync() condition resolution} — seals
 * every live stream at
 * once, letting their sourced reads run concurrently on the shared
 * {@link org.axonframework.eventsourcing.eventstore.EventStoreTransaction EventStoreTransaction}. That shared
 * transaction records a unified
 * {@link org.axonframework.eventsourcing.eventstore.ConsistencyMarker ConsistencyMarker} across all reads for the
 * DCB append step.
 * <p>
 * <h3>Supplementary reads.</h3>
 * A scope looked up after its stream was sealed receives a <em>fresh</em> stream: conditions declared
 * late are answered by a supplementary sourced read on the same transaction rather than rejected. Correctness and
 * the coverage guard are unaffected (every sourced read records its criteria in
 * {@link io.axoniq.framework.statecontroller.eventstream.ReadBoundaries ReadBoundaries}); the cost is one extra
 * round-trip per late batch, which is why the documented idiom is to declare all conditions before resolving any.
 * Conditions held on a reference to an already-sealed {@link EventStream} still fail fast with
 * {@link io.axoniq.framework.statecontroller.eventstream.LateConditionException LateConditionException} — the
 * leniency applies to scope lookups, not to stale stream references.
 * <p>
 * <b>Threading.</b> Instances are <em>not</em> thread-safe. AF5's command dispatch pipeline guarantees a single
 * {@link ProcessingContext} is processed by a single thread for the duration of an in-flight command, and this
 * session (along with the {@link SourcedEventStream}s it caches) relies on that contract.
 * <p>
 * Marked {@link Internal @Internal} because it is constructed by the framework's command dispatch pipeline; user
 * code obtains a {@link History} or {@link DecisionContext} as a handler method parameter.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
public final class HistorySession implements DecisionContext {

    private final EventStore eventStore;
    private final ProcessingContext processingContext;
    private final MessageTypeResolver typeResolver;
    private final Clock clock;
    private final Map<List<ScopeBranch>, SourcedEventStream> scopes = new HashMap<>();
    private final History root = new RootHistory(this);

    /**
     * Creates a {@code HistorySession} that will source events from the given {@code eventStore} as part of the
     * supplied {@code processingContext}.
     *
     * @param eventStore        the {@link EventStore} used to source events for declared conditions
     * @param processingContext the {@link ProcessingContext} the sourced reads participate in; its
     *                          application-context resolves the {@link MessageTypeResolver}
     * @param clock             the clock backing {@link #time()} so decision bodies stay deterministic under test
     */
    public HistorySession(EventStore eventStore, ProcessingContext processingContext, Clock clock) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.processingContext = Objects.requireNonNull(processingContext, "processingContext must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.typeResolver = processingContext.component(MessageTypeResolver.class);
    }

    /**
     * Returns the unbound root {@link History} of this session, the value injected into a state-controlled
     * {@code @CommandHandler}'s {@code History} parameter.
     *
     * @return the root history backed by this session
     */
    public History history() {
        return root;
    }

    /**
     * Returns the current {@link SourcedEventStream} for the given tag set, creating one when none exists yet or
     * when the cached stream has already been sealed (the supplementary-read case).
     *
     * @param tags the tag set identifying the scope
     * @return an unsealed event stream for the scope, ready to accept condition declarations
     */
    public SourcedEventStream scopeFor(Set<Tag> tags) {
        return scopeFor(List.of(ScopeBranch.unrestricted(tags)));
    }

    /**
     * Returns the current {@link SourcedEventStream} for the given branch list — a single branch for a simple
     * scope, several for a union scope — creating one when none exists yet or when the cached stream has already
     * been sealed (the supplementary-read case). Streams are cached per branch list, so two {@link History}
     * values describing the same branches share one stream and one sourced read.
     *
     * @param branches the branches identifying the scope; at least one required
     * @return an unsealed event stream for the scope, ready to accept condition declarations
     */
    public SourcedEventStream scopeFor(List<ScopeBranch> branches) {
        SourcedEventStream current = scopes.get(branches);
        if (current == null || current.isSealed()) {
            current = new SourcedEventStream(eventStore, processingContext, typeResolver, branches, this::sealAll);
            scopes.put(List.copyOf(branches), current);
        }
        return current;
    }

    /**
     * Seals every live stream of this session, kicking off their sourced reads concurrently. Invoked by the first
     * condition resolution of the in-flight decision; idempotent, and streams created afterwards (supplementary reads)
     * seal independently on their own first resolution.
     */
    public void sealAll() {
        // Copy: seal() completes conditions synchronously for empty scopes, which must not mutate the map mid-loop.
        List.copyOf(scopes.values()).forEach(SourcedEventStream::seal);
    }

    @Override
    public EventStream scope(String tagKey, Object tagValue) {
        Objects.requireNonNull(tagKey, "tagKey must not be null");
        Objects.requireNonNull(tagValue, "tagValue must not be null");
        return scopeFor(Set.of(new Tag(tagKey, tagValue.toString())));
    }

    @Override
    public EventStream scope(Map<String, ?> tags) {
        Objects.requireNonNull(tags, "tags must not be null");
        if (tags.isEmpty()) {
            throw new IllegalArgumentException("scope(...) requires at least one tag");
        }
        Set<Tag> tagSet = new HashSet<>(tags.size());
        for (Map.Entry<String, ?> e : tags.entrySet()) {
            tagSet.add(new Tag(
                    Objects.requireNonNull(e.getKey(), "tag key must not be null"),
                    Objects.requireNonNull(e.getValue(), "tag value must not be null").toString()));
        }
        return scopeFor(Set.copyOf(tagSet));
    }

    @Override
    public Instant time() {
        return clock.instant();
    }
}
