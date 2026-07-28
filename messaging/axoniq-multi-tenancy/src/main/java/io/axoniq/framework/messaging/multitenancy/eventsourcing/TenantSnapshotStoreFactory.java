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
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;

/**
 * Factory providing the {@link SnapshotStore} of a single {@link TenantDescriptor tenant}.
 * <p>
 * The multi-tenant snapshot store calls this factory to obtain the store to route a tenant's snapshot load and store
 * operations to. Implementations are expected to create the per-tenant store lazily and cache it.
 * <p>
 * A tenant whose {@link EventStorageEngine engine} resolves snapshots itself never reaches this factory while sourcing:
 * {@link TenantEventStorage} only asks for a snapshot store when the tenant's engine needs one.
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
