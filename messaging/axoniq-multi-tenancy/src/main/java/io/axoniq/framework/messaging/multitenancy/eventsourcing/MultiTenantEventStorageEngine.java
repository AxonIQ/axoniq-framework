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
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
 * A tenant-carrying processing context is required. When none is available, or the tenant cannot be resolved from it,
 * the operation fails. An append without a context resolves its tenant from the events instead, which must then all
 * belong to the same tenant.
 * <p>
 * As a {@link MultiTenantAwareComponent} this engine follows the
 * {@link io.axoniq.framework.messaging.multitenancy.api.TenantProvider TenantProvider}: a tenant added at runtime gets
 * its engine on first use, and a removed tenant's composed engine is evicted.
 * <p>
 * The read-side methods ({@link #stream}, {@link #firstToken}, {@link #latestToken}, {@link #tokenAt}) currently
 * throw an {@link UnsupportedOperationException}. Reading across all tenants is added together with the multi-tenant
 * pooled-streaming support, which merges the per-tenant streams behind these same methods.
 * TODO read-side methods will be resolved with #210
 *
 * @author Jakob Hatzl
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class MultiTenantEventStorageEngine implements EventStorageEngine, MultiTenantAwareComponent {

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
        throw streamingAcrossTenantsNotYetSupported();
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken() {
        throw streamingAcrossTenantsNotYetSupported();
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken() {
        throw streamingAcrossTenantsNotYetSupported();
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at) {
        throw streamingAcrossTenantsNotYetSupported();
    }

    private static UnsupportedOperationException streamingAcrossTenantsNotYetSupported() {
        return new UnsupportedOperationException("""
                Streaming across tenants is not supported yet. \
                It arrives with the multi-tenant pooled streaming support, \
                which merges the per-tenant streams behind this method.\
                """);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("engineFactory", engineFactory);
        descriptor.describeProperty("snapshotStoreFactory", snapshotStoreFactory);
        descriptor.describeProperty("tenantRouter", tenantRouter);
        composedEngines.describeTo(descriptor);
    }
}
