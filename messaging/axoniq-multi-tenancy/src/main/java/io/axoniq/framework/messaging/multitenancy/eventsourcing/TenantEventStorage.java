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

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SnapshotCapableEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;

import java.util.Objects;

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
 * Composing holds no state of its own, so this component takes no part in the tenant lifecycle. Each factory caches its
 * own per-tenant component and evicts it when the tenant is removed, and composing them again costs nothing: an engine
 * serving snapshots itself is returned as is, and any other engine is wrapped in two fields.
 *
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class TenantEventStorage implements DescribableComponent {

    private final TenantEventStorageEngineFactory engineFactory;
    private final TenantSnapshotStoreFactory snapshotStoreFactory;

    /**
     * Constructs a {@code TenantEventStorage} combining the engines and snapshot stores of the given factories.
     *
     * @param engineFactory        the factory providing each tenant's {@link EventStorageEngine}
     * @param snapshotStoreFactory the factory providing each tenant's {@link SnapshotStore}
     */
    public TenantEventStorage(TenantEventStorageEngineFactory engineFactory,
                              TenantSnapshotStoreFactory snapshotStoreFactory) {
        this.engineFactory = Objects.requireNonNull(engineFactory,
                                                   "The tenant event storage engine factory must not be null");
        this.snapshotStoreFactory = Objects.requireNonNull(snapshotStoreFactory,
                                                           "The tenant snapshot store factory must not be null");
    }

    /**
     * Returns the {@link EventStorageEngine} of the given {@code tenant}, able to resolve that tenant's snapshots.
     *
     * @param tenant the tenant to return the engine for
     * @return the given {@code tenant}'s engine, able to resolve that tenant's snapshots
     */
    public EventStorageEngine engineFor(TenantDescriptor tenant) {
        Objects.requireNonNull(tenant, "The tenant must not be null");
        return SnapshotCapableEventStorageEngine.decorate(engineFactory.engineFor(tenant),
                                                         snapshotStoreFactory.storeFor(tenant));
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("engineFactory", engineFactory);
        descriptor.describeProperty("snapshotStoreFactory", snapshotStoreFactory);
    }
}
