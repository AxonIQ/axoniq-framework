/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.eventsourcing;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;

/**
 * Factory providing the {@link EventStorageEngine} of a single {@link TenantDescriptor tenant}.
 * <p>
 * The multi-tenant event storage engine calls this factory to obtain the engine to route a tenant's writes and
 * sourcing to. Implementations are expected to create the per-tenant engine lazily and cache it.
 * <p>
 * Implementations build the tenant's engine and nothing more. Whether that engine also needs the tenant's snapshot
 * store to serve a snapshot sourcing strategy is decided by the {@link MultiTenantEventStorageEngine}, so an
 * implementation neither
 * knows nor cares about snapshots.
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
}
