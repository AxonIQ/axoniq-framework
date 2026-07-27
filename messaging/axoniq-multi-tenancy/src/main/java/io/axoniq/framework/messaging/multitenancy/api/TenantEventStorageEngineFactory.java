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

package io.axoniq.framework.messaging.multitenancy.api;

import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SnapshotCapableEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;

import java.util.Objects;

/**
 * Factory providing the {@link EventStorageEngine} of a single {@link TenantDescriptor tenant}.
 * <p>
 * The multi-tenant event storage engine calls this factory to obtain the engine to route a tenant's writes and
 * sourcing to. Implementations are expected to create the per-tenant engine lazily and cache it.
 * <p>
 * The returned engine must be able to resolve the tenant's snapshots, because the routing engine above it only routes
 * and never resolves a snapshot itself. Implementations that build an engine which is not its own {@link SnapshotStore}
 * therefore complement it with the tenant's snapshot store through
 * {@link #snapshotCapable(EventStorageEngine, SnapshotStore)}, once per tenant while the engine is created.
 *
 * @author Stefan Dragisic
 * @author Jakob Hatzl
 * @author Laura Devriendt
 * @since 4.6.0
 */
@FunctionalInterface
@Internal
public interface TenantEventStorageEngineFactory {

    /**
     * Returns the {@link EventStorageEngine} of the given {@code tenant}.
     *
     * @param tenant the tenant to return the event storage engine for
     * @return the tenant's {@link EventStorageEngine}
     */
    EventStorageEngine engineFor(TenantDescriptor tenant);

    /**
     * Returns an {@link EventStorageEngine} that resolves the snapshots of one tenant, given that tenant's
     * {@code tenantEngine} and {@code tenantSnapshotStore}.
     * <p>
     * An engine that is the tenant's snapshot store resolves the snapshot within its own
     * {@link EventStorageEngine#source source} call, so it is returned untouched and keeps that single round trip. Any
     * other engine is decorated with a {@link SnapshotCapableEventStorageEngine} reading from the tenant's snapshot
     * store, which resolves the snapshot before sourcing the events that follow it.
     * <p>
     * The two are compared by identity rather than by whether the engine implements {@link SnapshotStore}, mirroring
     * how the event sourcing defaults decide the same thing for a single-tenant engine. An engine that resolves
     * snapshots from its own storage, while the tenant's snapshots were written to a different store, still needs
     * decorating, or its {@link SourcingStrategy.Snapshot snapshot sourcing} would never find them.
     *
     * @param tenantEngine        the tenant's event storage engine
     * @param tenantSnapshotStore the snapshot store of the same tenant
     * @return an event storage engine resolving that tenant's snapshots
     * @throws NullPointerException if the given {@code tenantEngine} or {@code tenantSnapshotStore} is {@code null}
     */
    static EventStorageEngine snapshotCapable(EventStorageEngine tenantEngine, SnapshotStore tenantSnapshotStore) {
        Objects.requireNonNull(tenantEngine, "The tenant event storage engine must not be null");
        Objects.requireNonNull(tenantSnapshotStore, "The tenant snapshot store must not be null");
        return tenantSnapshotStore == tenantEngine
                ? tenantEngine
                : new SnapshotCapableEventStorageEngine(tenantEngine, tenantSnapshotStore);
    }
}
