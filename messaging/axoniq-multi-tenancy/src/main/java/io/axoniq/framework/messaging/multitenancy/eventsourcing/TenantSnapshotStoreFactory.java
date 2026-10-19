/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.eventsourcing;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;

/**
 * Factory providing the {@link SnapshotStore} of a single {@link TenantDescriptor tenant}.
 * <p>
 * The multi-tenant snapshot store calls this factory to obtain the store to route a tenant's snapshot load and store
 * operations to. Implementations are expected to create the per-tenant store lazily and cache it.
 * <p>
 * The {@link MultiTenantEventStorageEngine} asks for a tenant's snapshot store once, when it composes that tenant's
 * engine, and
 * compares the two to decide whether the engine needs the store at all. An engine that is its own snapshot store is the
 * one case where this factory is expected to return that very engine.
 *
 * @author Stefan Dragisic
 * @author Laura Devriendt
 * @since 5.3.0
 */
@FunctionalInterface
@Internal
public interface TenantSnapshotStoreFactory {

    /**
     * Returns the {@link SnapshotStore} of the given {@code tenant}.
     *
     * @param tenant the tenant to return the snapshot store for
     * @return the tenant's {@link SnapshotStore}
     */
    SnapshotStore storeFor(TenantDescriptor tenant);
}
