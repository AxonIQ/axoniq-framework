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

import io.axoniq.framework.messaging.multitenancy.api.RoutingTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptors;
import io.axoniq.framework.messaging.multitenancy.api.TenantEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
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
 * Tenant-routing {@link EventStorageEngine}. Writes and sourcing are routed to the engine of the one tenant resolved
 * from the {@link ProcessingContext}, so each tenant's events live in its own store.
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
public class MultiTenantEventStorageEngine implements EventStorageEngine {

    private final TenantEventStorageEngineFactory engineFactory;
    private final RoutingTenantResolver tenantResolver;

    /**
     * Constructs a {@code MultiTenantEventStorageEngine}.
     *
     * @param engineFactory  the factory providing each tenant's {@link EventStorageEngine}
     * @param tenantResolver the resolver determining the tenant of a message
     * @param tenants        the known tenants, used to resolve a message against
     */
    public MultiTenantEventStorageEngine(TenantEventStorageEngineFactory engineFactory,
                                         TenantResolver tenantResolver,
                                         TenantDescriptors tenants) {
        this.engineFactory = requireNonNull(engineFactory, "The tenant event storage engine factory must not be null");
        this.tenantResolver = new RoutingTenantResolver(tenantResolver, tenants);
    }

    @Override
    public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                @Nullable ProcessingContext context,
                                                                List<TaggedEventMessage<?>> events) {
        try {
            TenantDescriptor tenant = tenantForAppend(context, events);
            return engineFactory.engineFor(tenant).appendEvents(condition, context, events);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public MessageStream<EventMessage> source(SourcingCondition condition, @Nullable ProcessingContext context) {
        try {
            return engineFactory.engineFor(tenantFor(context)).source(condition, context);
        } catch (RuntimeException failure) {
            return MessageStream.failed(failure);
        }
    }

    private TenantDescriptor tenantForAppend(@Nullable ProcessingContext context, List<TaggedEventMessage<?>> events) {
        // Appends made inside a transaction (from a command or event handler) carry a processing context that already
        // holds the tenant, so it is routed like sourcing. A plain publish happens without a context, so the tenant is
        // instead resolved from the events themselves, which must all belong to the same tenant.
        if (context != null) {
            return tenantFor(context);
        }
        return tenantResolver.resolveSharedTenant(events.stream().map(TaggedEventMessage::event).toList())
                             .orElseThrow(tenantNotResolved("Tenant could not be resolved from the events to append"));
    }

    private TenantDescriptor tenantFor(@Nullable ProcessingContext context) {
        return tenantResolver.resolveFromContext(context)
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
                Streaming and token operations span all tenants rather than a single one, so they cannot be routed \
                from a single tenant's processing context. Reading across all tenants is not yet available on this \
                engine and is added together with the multi-tenant pooled-streaming support.""");
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("engineFactory", engineFactory);
        descriptor.describeProperty("tenantResolver", tenantResolver);
    }
}
