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

package io.axoniq.framework.messaging.multitenancy.eventsourcing;

import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.api.TenantScopedCache;
import io.axoniq.framework.messaging.multitenancy.eventstreaming.MultiTenantTrackingToken;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SnapshotCapableEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.DelayedMessageStream;
import org.axonframework.messaging.core.DelegatingMessageStream;
import org.axonframework.messaging.core.MergedMessageStream;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException.tenantNotResolved;
import static java.util.Objects.requireNonNull;

/**
 * Tenant-routing {@link EventStorageEngine}. Appends and sourcing are routed to the engine of the one tenant resolved
 * from the {@link ProcessingContext}, so each tenant's events live in its own store.
 * <p>
 * Like the other multi-tenant infrastructure components, this engine holds the tenant information, the registration of
 * tenants, and the routing to them. Snapshot writes are routed the same way by {@link MultiTenantSnapshotStore}, a
 * component of its own so that resolving an {@link EventStorageEngine} or a {@link SnapshotStore} by type stays
 * unambiguous.
 * <p>
 * The {@link SourcingCondition} is routed unchanged, so a {@link SourcingStrategy.Snapshot snapshot sourcing strategy}
 * reaches the tenant's own engine rather than being resolved above the fan-out, where no tenant is known yet. Keeping
 * it intact requires the application-wide snapshot composition to be switched off, which the configuration enhancer
 * registering this engine does.
 * <p>
 * Each tenant's engine is composed once with that tenant's snapshot store through
 * {@link SnapshotCapableEventStorageEngine#decorate(EventStorageEngine, SnapshotStore) decorate}, applying the same
 * rule the event sourcing defaults apply to a single-tenant engine. An engine that is its own snapshot store serves a
 * snapshot sourcing strategy within one call and is left untouched. Any other engine is decorated with that tenant's
 * snapshot store, resolving the snapshot first and sourcing the events following it. Both stay within one tenant.
 * <p>
 * A tenant-carrying processing context is required for appends and sourcing. When none is available, or the tenant
 * cannot be resolved from it, the operation fails. An append without a context resolves its tenant from the events
 * instead, which must then all belong to the same tenant.
 * <p>
 * As a {@link MultiTenantAwareComponent} this engine follows the
 * {@link io.axoniq.framework.messaging.multitenancy.api.TenantProvider TenantProvider}: a tenant added at runtime gets
 * its engine on first use, and a removed tenant's composed engine is evicted.
 * <p>
 * The read side ({@link #stream}, {@link #firstToken}, {@link #latestToken}, {@link #tokenAt}) carries no context and
 * spans all current tenants. It merges the per-tenant streams, tags every event with its tenant, and positions the
 * merged stream with a {@link MultiTenantTrackingToken} holding one position per tenant. A tenant added while a
 * processor runs streams from its beginning once the processor re-opens the stream.
 *
 * @author Jakob Hatzl
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class MultiTenantEventStorageEngine implements EventStorageEngine, MultiTenantAwareComponent {

    private static final Comparator<MessageStream.Entry<EventMessage>> OLDEST_FIRST =
            Comparator.comparing(entry -> entry.message().timestamp());

    private final TenantEventStorageEngineFactory engineFactory;
    private final TenantSnapshotStoreFactory snapshotStoreFactory;
    private final TenantRouter tenantRouter;
    private final TenantScopedCache<EventStorageEngine> composedEngines;

    /**
     * Constructs a {@code MultiTenantEventStorageEngine}.
     *
     * @param engineFactory        the factory providing each tenant's {@link EventStorageEngine}
     * @param snapshotStoreFactory the factory providing each tenant's {@link SnapshotStore}
     * @param tenantRouter         the router deciding which tenant an operation is routed to
     */
    public MultiTenantEventStorageEngine(TenantEventStorageEngineFactory engineFactory,
                                         TenantSnapshotStoreFactory snapshotStoreFactory,
                                         TenantRouter tenantRouter) {
        this.engineFactory = requireNonNull(engineFactory, "The tenant event storage engine factory must not be null");
        this.snapshotStoreFactory = requireNonNull(snapshotStoreFactory,
                                                   "The tenant snapshot store factory must not be null");
        this.tenantRouter = requireNonNull(tenantRouter, "The tenant router must not be null");
        this.composedEngines = new TenantScopedCache<>(this::compose, "the multi-tenant event storage engine");
    }

    @Override
    public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                @Nullable ProcessingContext context,
                                                                List<TaggedEventMessage<?>> events) {
        try {
            TenantDescriptor tenant = tenantForAppend(context, events);
            return engineFor(tenant).appendEvents(condition, context, events);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public MessageStream<EventMessage> source(SourcingCondition condition, @Nullable ProcessingContext context) {
        try {
            return engineFor(tenantFor(context)).source(condition, context);
        } catch (RuntimeException failure) {
            return MessageStream.failed(failure);
        }
    }

    /**
     * Returns the complete {@link EventStorageEngine} of the given {@code tenant}, able to resolve that tenant's
     * snapshots, composing and caching it on first use.
     * <p>
     * Not private, because the per-tenant read side merges these same engines.
     *
     * @param tenant the tenant to return the engine of
     * @return the engine of the given {@code tenant}
     * @throws io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException if the tenant is not registered
     */
    EventStorageEngine engineFor(TenantDescriptor tenant) {
        return composedEngines.componentFor(tenant);
    }

    private EventStorageEngine compose(TenantDescriptor tenant) {
        return SnapshotCapableEventStorageEngine.decorate(engineFactory.engineFor(tenant),
                                                          snapshotStoreFactory.storeFor(tenant));
    }

    /**
     * Returns the tenants currently registered with {@code this} engine.
     *
     * @return the tenants currently registered with {@code this} engine
     */
    public List<TenantDescriptor> tenants() {
        return composedEngines.tenants();
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        return composedEngines.registerTenant(tenantDescriptor);
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        return composedEngines.registerAndStartTenant(tenantDescriptor);
    }

    private TenantDescriptor tenantForAppend(@Nullable ProcessingContext context, List<TaggedEventMessage<?>> events) {
        // Appends made inside a transaction (from a command or event handler) carry a processing context that already
        // holds the tenant, so it is routed like sourcing. A plain publish happens without a context, so the tenant is
        // instead resolved from the events themselves, which must all belong to the same tenant.
        if (context != null) {
            return tenantFor(context);
        }
        return tenantRouter.resolveSharedTenant(events.stream().map(TaggedEventMessage::event).toList())
                           .orElseThrow(tenantNotResolved("Tenant could not be resolved from the events to append"));
    }

    private TenantDescriptor tenantFor(@Nullable ProcessingContext context) {
        return tenantRouter.resolveFromContext(context)
                .orElseThrow(tenantNotResolved("Tenant could not be resolved from the processing context"));
    }

    @Override
    public MessageStream<EventMessage> stream(StreamingCondition condition) {
        try {
            List<TenantDescriptor> currentTenants = tenants();
            if (currentTenants.isEmpty()) {
                return MessageStream.empty();
            }
            MultiTenantTrackingToken startToken = MultiTenantTrackingToken.from(condition.position());
            return DelayedMessageStream.create(
                    resolveStartToken(startToken, currentTenants)
                            .thenApply(openFrom -> merge(startToken, openFrom, currentTenants, condition.criteria())));
        } catch (RuntimeException failure) {
            // Resolving the start position touches each tenant's engine and can fail synchronously. Return that as a
            // failed stream rather than throwing, matching source() and appendEvents().
            return MessageStream.failed(failure);
        }
    }

    /**
     * Fills in a per-tenant beginning position for every current tenant absent from the {@code startToken}, so a
     * tenant added since the token was written streams from its beginning rather than being skipped.
     */
    private CompletableFuture<MultiTenantTrackingToken> resolveStartToken(MultiTenantTrackingToken startToken,
                                                                          List<TenantDescriptor> currentTenants) {
        CompletableFuture<MultiTenantTrackingToken> start = CompletableFuture.completedFuture(startToken);
        for (TenantDescriptor tenant : currentTenants) {
            if (startToken.tokenForTenant(tenant.tenantId()) == null) {
                start = start.thenCombine(
                        engineFor(tenant).firstToken(),
                        (token, firstToken) -> token.advancedTo(tenant.tenantId(), firstToken));
            }
        }
        return start;
    }

    /**
     * Merges the per-tenant streams, opening each from {@code openFrom} but positioning the merged entries from
     * {@code carriedToken}.
     * <p>
     * The two differ for a tenant that has no position yet: {@code openFrom} holds the beginning of that tenant's store
     * so its stream opens there, while {@code carriedToken} leaves it out. Absent already means "nothing consumed", so
     * leaving it out loses nothing, and it keeps a position the processor never reached out of the emitted tokens.
     * <p>
     * That matters because a streaming processor compares a stored token against an emitted one to recognize an event it
     * already handled. Naming a tenant that the stored token predates makes every such comparison fail, and the
     * processor hands events it had already handled to its handlers a second time.
     */
    private MessageStream<EventMessage> merge(MultiTenantTrackingToken carriedToken,
                                              MultiTenantTrackingToken openFrom,
                                              List<TenantDescriptor> currentTenants,
                                              EventCriteria criteria) {
        Iterator<TenantDescriptor> tenantIterator = currentTenants.iterator();
        MessageStream<EventMessage> merged = openTenantStream(tenantIterator.next(), openFrom, criteria);
        try {
            while (tenantIterator.hasNext()) {
                MessageStream<EventMessage> tenantStream = openTenantStream(tenantIterator.next(), openFrom, criteria);
                merged = new MergedMessageStream<>(OLDEST_FIRST, merged, tenantStream);
            }
        } catch (RuntimeException openFailure) {
            // Opening a later tenant's stream failed. Close the streams already opened for the earlier tenants before
            // rethrowing, so a failed open does not leak them. A failure while closing must not mask the open failure.
            try {
                merged.close();
            } catch (RuntimeException closeFailure) {
                openFailure.addSuppressed(closeFailure);
            }
            throw openFailure;
        }
        return new TenantPositioningStream(carriedToken, merged);
    }

    private MessageStream<EventMessage> openTenantStream(TenantDescriptor tenant,
                                                         MultiTenantTrackingToken openFrom,
                                                         EventCriteria criteria) {
        StreamingCondition tenantCondition =
                StreamingCondition.conditionFor(openFrom.tokenForTenant(tenant.tenantId()), criteria);
        return engineFor(tenant)
                .stream(tenantCondition)
                .map(entry -> entry.withResource(TenantDescriptor.RESOURCE_KEY, tenant));
    }

    /**
     * Returns a token holding no per-tenant position, so a processor resuming from it opens every tenant at the
     * beginning of its own store.
     * <p>
     * The beginning of a tenant's store is not assumed to be the zero position. {@link #stream(StreamingCondition)
     * Streaming} from this token fills in each tenant's own {@link EventStorageEngine#firstToken() first token} when it
     * opens that tenant's stream, so a tenant whose early events were pruned still opens at its real first event.
     * Naming those positions in this token instead would make it disagree with a token written before a tenant existed,
     * and a streaming processor compares the two to recognize an event it already handled, so they are left out.
     */
    @Override
    public CompletableFuture<TrackingToken> firstToken() {
        return CompletableFuture.completedFuture(MultiTenantTrackingToken.empty());
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken() {
        return composeToken(EventStorageEngine::latestToken);
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at) {
        return composeToken(engine -> engine.tokenAt(at));
    }

    private CompletableFuture<TrackingToken> composeToken(
            Function<EventStorageEngine, CompletableFuture<TrackingToken>> tokenLookup) {
        try {
            CompletableFuture<MultiTenantTrackingToken> composed =
                    CompletableFuture.completedFuture(MultiTenantTrackingToken.empty());
            for (TenantDescriptor tenant : tenants()) {
                composed = composed.thenCombine(
                        tokenLookup.apply(engineFor(tenant)),
                        (token, resolved) -> token.advancedTo(tenant.tenantId(), resolved));
            }
            // Upcast CompletableFuture<MultiTenantTrackingToken> to the CompletableFuture<TrackingToken> return type.
            return composed.thenApply(TrackingToken.class::cast);
        } catch (RuntimeException failure) {
            // Resolving a tenant's token touches its engine and can fail synchronously. Return a failed future rather
            // than throwing, matching appendEvents().
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("engineFactory", engineFactory);
        descriptor.describeProperty("snapshotStoreFactory", snapshotStoreFactory);
        descriptor.describeProperty("tenantRouter", tenantRouter);
        composedEngines.describeTo(descriptor);
    }

    /**
     * Wraps the merged per-tenant stream to position each entry with a {@link MultiTenantTrackingToken}, advancing the
     * emitting tenant to the position its own engine put on the entry.
     */
    private static class TenantPositioningStream extends DelegatingMessageStream<EventMessage, EventMessage> {

        private final AtomicReference<MultiTenantTrackingToken> currentToken;

        private TenantPositioningStream(MultiTenantTrackingToken startToken, MessageStream<EventMessage> delegate) {
            super(delegate);
            this.currentToken = new AtomicReference<>(startToken);
        }

        @Override
        public Optional<Entry<EventMessage>> next() {
            return delegate().next().map(entry -> entry.withResource(
                    TrackingToken.RESOURCE_KEY,
                    currentToken.updateAndGet(token -> advance(token, entry))));
        }

        @Override
        public Optional<Entry<EventMessage>> peek() {
            return delegate().peek().map(entry -> entry.withResource(
                    TrackingToken.RESOURCE_KEY,
                    advance(currentToken.get(), entry)));
        }

        private static MultiTenantTrackingToken advance(MultiTenantTrackingToken token, Entry<EventMessage> entry) {
            TenantDescriptor tenant = requireNonNull(entry.getResource(TenantDescriptor.RESOURCE_KEY),
                                                     "The merged entry must carry its tenant");
            TrackingToken tenantToken = requireNonNull(entry.getResource(TrackingToken.RESOURCE_KEY),
                                                       "The merged entry must carry its tenant's position");
            return token.advancedTo(tenant.tenantId(), tenantToken);
        }
    }
}
