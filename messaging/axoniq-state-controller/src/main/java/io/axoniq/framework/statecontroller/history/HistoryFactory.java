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

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Per-{@link ProcessingContext} factory minting and caching the {@link History} instances a decision reads.
 * <p>
 * Holds the {@link EventStore}, the in-flight {@link ProcessingContext}, the resolved {@link MessageTypeResolver},
 * and an optional {@link Converter}. {@link #narrow(EventCriteria)} returns a {@link SourcedHistory} per distinct
 * {@link EventCriteria}, so two reads narrowed to the same scope share one materialized snapshot — and therefore
 * one sourced read and one recorded DCB consistency marker. This mirrors the scope caching that
 * {@code DecisionContextImpl} performs for the existing {@code EventStream} surface.
 * <p>
 * {@link #rootHistoryFor(ProcessingContext)} is the framework entry point: it caches a single {@link RootHistory}
 * (backed by one {@code HistoryFactory}) under a private {@link ResourceKey} on the processing context, so every
 * parameter resolver or downstream lookup for the in-flight command observes the same factory and the same
 * narrowed-history cache.
 * <p>
 * <h3>Threading.</h3>
 * Instances are <em>not</em> thread-safe; they rely on AF5's single-thread-per-{@link ProcessingContext}
 * guarantee and mutate the criteria cache without synchronization.
 * <p>
 * Marked {@link Internal @Internal} because it is wired by the framework's command dispatch pipeline; user code
 * obtains a {@link History} as a {@code @Decide} method parameter.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
@Internal
public final class HistoryFactory {

    /**
     * Resource key under which the shared {@link RootHistory} for a given {@link ProcessingContext} is cached.
     * Private because external lookup should go through {@link #rootHistoryFor(ProcessingContext)}.
     */
    private static final ResourceKey<History> ROOT_HISTORY_KEY =
            ResourceKey.withLabel("io.axoniq.framework.statecontroller.History");

    private final EventStore eventStore;
    private final ProcessingContext processingContext;
    private final MessageTypeResolver typeResolver;
    private final @Nullable Converter converter;
    private final Map<EventCriteria, SourcedHistory> narrowed = new HashMap<>();

    /**
     * Creates a {@code HistoryFactory} sourcing events from {@code eventStore} as part of
     * {@code processingContext}.
     *
     * @param eventStore        the {@link EventStore} narrowed histories source their slices from
     * @param processingContext the {@link ProcessingContext} the sourced reads participate in; resolves the
     *                          {@link MessageTypeResolver} and an optional {@link Converter}
     */
    public HistoryFactory(EventStore eventStore, ProcessingContext processingContext) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.processingContext = Objects.requireNonNull(processingContext, "processingContext must not be null");
        this.typeResolver = processingContext.component(MessageTypeResolver.class);
        this.converter = resolveConverter(processingContext);
    }

    /**
     * Returns the shared root {@link History} for the given {@code processingContext}, creating it (and its
     * backing {@code HistoryFactory}) on first call. Subsequent invocations on the same {@code ProcessingContext}
     * return the same instance, so all reads during the in-flight command share one narrowed-history cache and
     * one recorded DCB consistency marker.
     *
     * @param processingContext the current processing context
     * @return the shared unbound {@link History} for this processing context
     */
    public static History rootHistoryFor(ProcessingContext processingContext) {
        Objects.requireNonNull(processingContext, "processingContext must not be null");
        return processingContext.computeResourceIfAbsent(
                ROOT_HISTORY_KEY,
                () -> new RootHistory(new HistoryFactory(processingContext.component(EventStore.class),
                                                         processingContext)));
    }

    /**
     * Returns a narrowed {@link History} for the given {@code criteria}, reusing an existing one when this
     * factory has already minted a {@link SourcedHistory} for the same {@link EventCriteria}.
     *
     * @param criteria the {@link EventCriteria} defining the scope to narrow to
     * @return a {@link SourcedHistory} bound to {@code criteria}
     */
    History narrow(EventCriteria criteria) {
        Objects.requireNonNull(criteria, "criteria must not be null");
        return narrowed.computeIfAbsent(
                criteria,
                key -> new SourcedHistory(eventStore, processingContext, typeResolver, converter, key));
    }

    /**
     * Mints a fresh fluent-builder {@link History} whose first, tagless term is restricted to {@code types},
     * matching events of those types across all tags. Unlike {@link #narrow(EventCriteria)}, the result is not
     * cached: an under-construction builder has no final {@link EventCriteria} yet, and its terms are folded into
     * one only on the first read.
     *
     * @param types the event payload classes the first, tagless term is restricted to
     * @return a {@link SourcedHistory} builder whose first term matches {@code types} across all tags
     * @throws IllegalArgumentException if {@code types} is empty
     */
    History ofTypes(Class<?>... types) {
        return SourcedHistory.ofTypes(eventStore, processingContext, typeResolver, converter, types);
    }

    /**
     * Resolves a {@link Converter} from the surrounding {@link ProcessingContext}, returning {@code null} if none
     * is registered. Prefers {@link GeneralConverter}, the canonical low-level converter; looking up the bare
     * {@link Converter} type alone is ambiguous because {@code MessageConverter} and {@code EventConverter} also
     * implement it. A {@code null} converter is valid input to
     * {@link org.axonframework.messaging.eventhandling.EventMessage#payloadAs(Class, Converter) payloadAs} when
     * the payload is already an instance of the requested type — typical for in-memory test fixtures.
     */
    private static @Nullable Converter resolveConverter(ProcessingContext context) {
        try {
            return context.component(GeneralConverter.class);
        } catch (ComponentNotFoundException notFound) {
            return null;
        }
    }
}
