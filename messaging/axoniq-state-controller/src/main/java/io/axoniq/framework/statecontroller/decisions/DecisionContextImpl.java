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

package io.axoniq.framework.statecontroller.decisions;

import io.axoniq.framework.statecontroller.eventstream.EventStream;
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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Runtime {@link DecisionContext} that wires a state controller invocation to an Axon Framework {@link EventStore}.
 * <p>
 * Each {@link #scope scope(...)} call returns a {@link SourcedEventStream} for the requested tag set. The same tag
 * set returns the same stream (cached on this context) so multiple lookups for, say,
 * {@code scope("account", "a1")} register all their conditions against the same scope and load through a single
 * sourced read. Distinct scopes are independent: each owns its own seal/loaded-events state but all share the
 * same {@link org.axonframework.eventsourcing.eventstore.EventStoreTransaction EventStoreTransaction} via
 * {@link EventStore#transaction(ProcessingContext)} caching, so a unified
 * {@link org.axonframework.eventsourcing.eventstore.ConsistencyMarker ConsistencyMarker} is recorded across all
 * per-scope reads for the Phase 4 DCB append step.
 * <p>
 * The {@link MessageTypeResolver} used to map registered payload classes to
 * {@link org.axonframework.messaging.core.QualifiedName QualifiedName} entries on the combined
 * {@link org.axonframework.messaging.eventstreaming.EventCriteria EventCriteria} is looked up from the supplied
 * {@code processingContext} via
 * {@link org.axonframework.messaging.core.ApplicationContext#component(Class) component(Class)}; the resolver
 * therefore honours any {@code @Event(name = ...)} (or compatible
 * {@link org.axonframework.messaging.core.annotation.Message Message}-meta-annotated) declarations on event
 * payload classes.
 * <p>
 * <b>Threading.</b> Instances are <em>not</em> thread-safe. AF5's command dispatch pipeline guarantees a single
 * {@link ProcessingContext} is processed by a single thread for the duration of an in-flight command, and
 * {@code DecisionContextImpl} (along with the {@link SourcedEventStream}s it caches) relies on that contract:
 * the scope cache is mutated without synchronization. User code should never hand a {@code DecisionContext}
 * to another thread.
 * <p>
 * Marked {@link Internal @Internal} because it is constructed by the framework's command dispatch pipeline
 * (Phase 3 wiring); user code obtains a {@code DecisionContext} as a method parameter on a
 * {@link StateController @StateController} method or as the second argument to a declarative
 * {@link StateControllerComponent} decision function.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
public final class DecisionContextImpl implements DecisionContext {

    private final EventStore eventStore;
    private final ProcessingContext processingContext;
    private final MessageTypeResolver typeResolver;
    private final Clock clock;
    private final Map<Set<Tag>, SourcedEventStream> scopes = new HashMap<>();

    /**
     * Creates a {@code DecisionContextImpl} that will source events from the given {@code eventStore} as part of
     * the supplied {@code processingContext}.
     *
     * @param eventStore        the {@link EventStore} used to source events for declared conditions
     * @param processingContext the {@link ProcessingContext} the sourced read participates in. Its
     *                          {@link org.axonframework.eventsourcing.eventstore.ConsistencyMarker
     *                          ConsistencyMarker} is recorded for the Phase 4 DCB append, and its
     *                          application-context resolves the {@code MessageTypeResolver}
     * @param clock             the clock used by {@link #time()} so decision bodies stay deterministic under test
     */
    public DecisionContextImpl(EventStore eventStore,
                               ProcessingContext processingContext,
                               Clock clock) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.processingContext = Objects.requireNonNull(processingContext, "processingContext must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.typeResolver = processingContext.component(MessageTypeResolver.class);
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

    private SourcedEventStream scopeFor(Set<Tag> tags) {
        return scopes.computeIfAbsent(tags,
                                      key -> new SourcedEventStream(eventStore,
                                                                 processingContext,
                                                                 typeResolver,
                                                                 key));
    }
}
