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

/**
 * Factory providing the {@link EventStorageEngine} of a single {@link TenantDescriptor tenant}.
 * <p>
 * The multi-tenant event storage engine calls this factory to obtain the engine to route a tenant's writes and
 * sourcing to. Implementations are expected to create the per-tenant engine lazily and cache it.
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
