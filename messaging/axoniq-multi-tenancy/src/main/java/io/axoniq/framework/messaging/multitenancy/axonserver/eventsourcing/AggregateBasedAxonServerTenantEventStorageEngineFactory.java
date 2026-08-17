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

package io.axoniq.framework.messaging.multitenancy.axonserver.eventsourcing;

import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.AggregateBasedAxonServerEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentLookup;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantScopedCache;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantEventStorageEngineFactory;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Axon Server implementation of the {@link TenantEventStorageEngineFactory}, building one
 * {@link AggregateBasedAxonServerEventStorageEngine} per tenant against that tenant's Axon Server context.
 * <p>
 * This factory is for tenants backed by Axon Server contexts without Dynamic Consistency Boundary support. The
 * aggregate-based engine is created lazily and cached, and evicted on tenant removal, by a
 * {@link TenantScopedCache} this factory holds. Following the tenant lifecycle is delegated to that cache through
 * {@link MultiTenantAwareComponent}.
 *
 * @author Jan Galinski
 * @since 5.4.0
 */
@Internal
public class AggregateBasedAxonServerTenantEventStorageEngineFactory
        implements TenantEventStorageEngineFactory, MultiTenantAwareComponent {

    private final TenantScopedCache<EventStorageEngine> engineCache;

    /**
     * Constructs an {@code AggregateBasedAxonServerTenantEventStorageEngineFactory} building per-tenant aggregate
     * storage engines from the given components.
     *
     * @param connectionManager the connection manager providing each tenant's Axon Server connection
     * @param defaultConverter the converter used when no tenant-specific converter is configured
     * @param eventTypeResolver the resolver for the event types stored in each tenant's context
     */
    public AggregateBasedAxonServerTenantEventStorageEngineFactory(
            AxonServerConnectionManager connectionManager,
            EventConverter defaultConverter,
            EventTypeResolver eventTypeResolver) {
        this(connectionManager, defaultConverter, eventTypeResolver, null);
    }

    /**
     * Constructs an {@code AggregateBasedAxonServerTenantEventStorageEngineFactory} building per-tenant aggregate
     * storage engines from the given components.
     *
     * @param connectionManager the connection manager providing each tenant's Axon Server connection
     * @param defaultConverter the converter used when no tenant-specific converter is configured
     * @param eventTypeResolver the resolver for the event types stored in each tenant's context
     * @param tenantConverterProvider the optional provider of tenant-specific converters
     */
    public AggregateBasedAxonServerTenantEventStorageEngineFactory(
            AxonServerConnectionManager connectionManager,
            EventConverter defaultConverter,
            EventTypeResolver eventTypeResolver,
            @Nullable TenantComponentProvider<Converter> tenantConverterProvider) {
        this.engineCache = new TenantScopedCache<>(perTenantEngine(connectionManager,
                                                                    defaultConverter,
                                                                    eventTypeResolver,
                                                                    tenantConverterProvider),
                                                  "the tenant aggregate-based event storage engine factory");
    }

    @Override
    public EventStorageEngine engineFor(TenantDescriptor tenant) {
        return engineCache.componentFor(tenant);
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        return engineCache.registerTenant(tenantDescriptor);
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        return engineCache.registerAndStartTenant(tenantDescriptor);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        engineCache.describeTo(descriptor);
    }

    private static TenantComponentLookup<EventStorageEngine> perTenantEngine(
            AxonServerConnectionManager connectionManager,
            EventConverter defaultConverter,
            EventTypeResolver eventTypeResolver,
            @Nullable TenantComponentProvider<Converter> tenantConverterProvider) {
        Objects.requireNonNull(connectionManager, "The connection manager must not be null");
        Objects.requireNonNull(defaultConverter, "The default converter must not be null");
        Objects.requireNonNull(eventTypeResolver, "The event type resolver must not be null");
        return tenant -> new AggregateBasedAxonServerEventStorageEngine(
                connectionManager.getConnection(tenant.tenantId()),
                tenantConverterProvider != null
                        ? new DelegatingEventConverter(tenantConverterProvider.componentFor(tenant))
                        : defaultConverter,
                eventTypeResolver
        );
    }
}
