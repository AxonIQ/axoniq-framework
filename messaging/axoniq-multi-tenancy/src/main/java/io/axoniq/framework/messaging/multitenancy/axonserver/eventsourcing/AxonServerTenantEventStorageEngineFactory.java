/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.axonserver.eventsourcing;

import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantScopedCache;
import io.axoniq.framework.messaging.multitenancy.configuration.TenantComponentProviderUtil;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantEventStorageEngineFactory;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Axon Server implementation of the {@link TenantEventStorageEngineFactory}, building one {@link EventStorageEngine}
 * per tenant against that tenant's Axon Server context.
 * <p>
 * Axon Server's engine is not its own {@link SnapshotStore} and does not resolve snapshots while sourcing, so
 * {@link io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine} complements it with
 * that tenant's
 * snapshot store. This factory only builds the engine.
 * <p>
 * Engines are created lazily and cached, and evicted on tenant removal, by a {@link TenantScopedCache} this factory
 * holds. Following the tenant lifecycle is delegated to that cache through {@link MultiTenantAwareComponent}. The
 * underlying connection is owned by the connection manager, which disconnects it on tenant removal, so eviction only
 * drops the stale engine and a re-added tenant rebuilds a fresh one.
 *
 * @author Jakob Hatzl
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class AxonServerTenantEventStorageEngineFactory
        implements TenantEventStorageEngineFactory, MultiTenantAwareComponent {

    private final TenantScopedCache<EventStorageEngine> engineCache;

    /**
     * Constructs an {@code AxonServerTenantEventStorageEngineFactory} building per-tenant engines from the given
     * {@code configuration}.
     *
     * @param configuration the configuration used to construct each tenant's Axon Server event storage engine
     */
    public AxonServerTenantEventStorageEngineFactory(Configuration configuration) {
        Objects.requireNonNull(configuration, "The configuration must not be null");
        this.engineCache = new TenantScopedCache<>(perTenantEngine(configuration),
                                                  "the tenant event storage engine factory");
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

    private static Function<TenantDescriptor, EventStorageEngine> perTenantEngine(Configuration configuration) {
        EventConverter defaultConverter = configuration.getComponent(EventConverter.class);
        Optional<TenantComponentProvider<Converter>> tenantConverters =
                TenantComponentProviderUtil.find(configuration, Converter.class);
        return tenant -> AxonServerEventStorageEngineFactory.constructForContext(
                tenant.tenantId(),
                configuration,
                tenantConverters.<EventConverter>map(
                                        provider -> new DelegatingEventConverter(provider.componentFor(tenant)))
                                .orElse(defaultConverter)
        );
    }
}
