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

package io.axoniq.framework.messaging.multitenancy.axonserver;

import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantScopedCache;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantEventStorageEngineFactory;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;

import java.util.Objects;
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
        return tenant -> AxonServerEventStorageEngineFactory.constructForContext(tenant.tenantId(), configuration);
    }
}
