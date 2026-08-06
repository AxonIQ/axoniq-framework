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
import io.axoniq.framework.axonserver.connector.snapshot.AxonServerSnapshotStore;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantScopedCache;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantSnapshotStoreFactory;
import io.axoniq.framework.messaging.multitenancy.configuration.TenantComponentProviders;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Axon Server implementation of the {@link TenantSnapshotStoreFactory}, building one {@link SnapshotStore} per
 * tenant against that tenant's Axon Server context.
 * <p>
 * Stores are created lazily and cached, and evicted on tenant removal, by a {@link TenantScopedCache} this factory
 * holds. Following the tenant lifecycle is delegated to that cache through {@link MultiTenantAwareComponent}. The
 * underlying connection is owned by the connection manager, which disconnects it on tenant removal, so eviction only
 * drops the stale store and a re-added tenant rebuilds a fresh one.
 *
 * @author Jakob Hatzl
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class AxonServerTenantSnapshotStoreFactory
        implements TenantSnapshotStoreFactory, MultiTenantAwareComponent {

    private final TenantScopedCache<SnapshotStore> storeCache;

    /**
     * Constructs an {@code AxonServerTenantSnapshotStoreFactory}, resolving the connection manager and converter from
     * the given {@code configuration}.
     *
     * @param configuration the configuration providing the connection manager and converter
     */
    public AxonServerTenantSnapshotStoreFactory(Configuration configuration) {
        this(configuration.getComponent(AxonServerConnectionManager.class),
             configuration.getComponent(GeneralConverter.class),
             TenantComponentProviders.find(configuration, Converter.class));
    }

    /**
     * Constructs an {@code AxonServerTenantSnapshotStoreFactory}.
     *
     * @param connectionManager the connection manager providing each tenant's Axon Server connection
     * @param converter         the converter used to (de)serialize snapshot payloads
     */
    public AxonServerTenantSnapshotStoreFactory(AxonServerConnectionManager connectionManager, Converter converter) {
        this(connectionManager, converter, Optional.empty());
    }

    private AxonServerTenantSnapshotStoreFactory(AxonServerConnectionManager connectionManager,
                                                 Converter defaultConverter,
                                                 Optional<TenantComponentProvider<Converter>> tenantConverters) {
        this.storeCache = new TenantScopedCache<>(perTenantStore(connectionManager, defaultConverter, tenantConverters),
                                                 "the tenant snapshot store factory");
    }

    @Override
    public SnapshotStore storeFor(TenantDescriptor tenant) {
        return storeCache.componentFor(tenant);
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        return storeCache.registerTenant(tenantDescriptor);
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        return storeCache.registerAndStartTenant(tenantDescriptor);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        storeCache.describeTo(descriptor);
    }

    private static Function<TenantDescriptor, SnapshotStore> perTenantStore(
            AxonServerConnectionManager connectionManager,
            Converter defaultConverter,
            Optional<TenantComponentProvider<Converter>> tenantConverters) {
        Objects.requireNonNull(connectionManager, "The connection manager must not be null");
        Objects.requireNonNull(defaultConverter, "The converter must not be null");
        return tenant -> new AxonServerSnapshotStore(
                connectionManager.getConnection(tenant.tenantId()),
                tenantConverters.map(provider -> provider.componentFor(tenant)).orElse(defaultConverter)
        );
    }
}
