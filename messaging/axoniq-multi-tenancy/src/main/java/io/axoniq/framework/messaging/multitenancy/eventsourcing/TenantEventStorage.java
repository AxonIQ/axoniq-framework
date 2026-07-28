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
import io.axoniq.framework.messaging.multitenancy.api.TenantScopedCache;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SnapshotCapableEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;

import static java.util.Objects.requireNonNull;

/**
 * Provides the complete {@link EventStorageEngine} of a single tenant: the tenant's engine, able to resolve that
 * tenant's snapshots.
 * <p>
 * A tenant's events and snapshots come from two separate factories, and how they combine depends on the backend.
 * {@link SnapshotCapableEventStorageEngine#decorate(EventStorageEngine, SnapshotStore) Decorating} applies the same
 * rule the event sourcing defaults apply to a single-tenant engine: an engine that is its own snapshot store serves a
 * {@link SourcingStrategy.Snapshot snapshot sourcing strategy} within one call and is left untouched, while any other
 * engine is decorated with that tenant's snapshot store, resolving the snapshot first and sourcing the events following
 * it. Both are correct, and both stay within one tenant.
 * <p>
 * Composing here, once per tenant, is what lets the {@link MultiTenantEventStorageEngine} above it do nothing but
 * route: it forwards the sourcing condition unchanged and every tenant's engine arrives ready to handle it. The
 * application-wide composition is switched off for that reason, by the configuration enhancer registering these
 * components.
 * <p>
 * A tenant's engine is composed once and cached, and evicted when the tenant is removed, so sourcing neither composes
 * anew on every call nor hands out a different instance each time. Composing asks both factories for that tenant's
 * component, so a tenant is never charged for a snapshot store its engine does not need beyond the first composition.
 *
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class TenantEventStorage implements MultiTenantAwareComponent {

    private final TenantEventStorageEngineFactory engineFactory;
    private final TenantSnapshotStoreFactory snapshotStoreFactory;
    private final TenantScopedCache<EventStorageEngine> composedEngines;

    /**
     * Constructs a {@code TenantEventStorage} combining the engines and snapshot stores of the given factories.
     *
     * @param engineFactory        the factory providing each tenant's {@link EventStorageEngine}
     * @param snapshotStoreFactory the factory providing each tenant's {@link SnapshotStore}
     */
    public TenantEventStorage(TenantEventStorageEngineFactory engineFactory,
                              TenantSnapshotStoreFactory snapshotStoreFactory) {
        this.engineFactory = requireNonNull(engineFactory,
                                            "The tenant event storage engine factory must not be null");
        this.snapshotStoreFactory = requireNonNull(snapshotStoreFactory,
                                                   "The tenant snapshot store factory must not be null");
        this.composedEngines = new TenantScopedCache<>(this::compose, "the tenant event storage");
    }

    /**
     * Returns the {@link EventStorageEngine} of the given {@code tenant}, able to resolve that tenant's snapshots.
     *
     * @param tenant the tenant to return the engine for
     * @return the given {@code tenant}'s engine, able to resolve that tenant's snapshots
     */
    public EventStorageEngine composedEngineFor(TenantDescriptor tenant) {
        requireNonNull(tenant, "The tenant must not be null");
        return composedEngines.componentFor(tenant);
    }

    private EventStorageEngine compose(TenantDescriptor tenant) {
        return SnapshotCapableEventStorageEngine.decorate(engineFactory.engineFor(tenant),
                                                         snapshotStoreFactory.storeFor(tenant));
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        return composedEngines.registerTenant(tenantDescriptor);
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        return composedEngines.registerAndStartTenant(tenantDescriptor);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("engineFactory", engineFactory);
        descriptor.describeProperty("snapshotStoreFactory", snapshotStoreFactory);
        composedEngines.describeTo(descriptor);
    }
}
