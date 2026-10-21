/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;

/**
 * A functional interface resolving the stream id for a multi-tenant persistent stream from the supplied
 * configured name and {@link TenantDescriptor}. This can be used to customize the identity of individual persistent
 * streams per tenant.
 * <p>
 * Implementations must be deterministic and stable across restarts: the stream id is the stream's identity on Axon
 * Server, so resolving a different id for a tenant opens a new stream, which replays that tenant's events from the
 * stream's initial position. Resolved ids must also be distinct per configured stream within one tenant, since sources
 * sharing a stream id in the same context join the same server-side stream.
 *
 * @author Jakob Hatzl
 * @see MultiTenantPersistentStreamEventSourceFactory
 * @since 5.4.1
 */
@FunctionalInterface
public interface MultiTenantPersistentStreamIdResolver {

    /**
     * The default resolver, identifying every tenant's stream by the configured stream name as-is.
     */
    MultiTenantPersistentStreamIdResolver CONFIGURED_NAME = (name, tenant) -> name;

    /**
     * Resolves the stream id under which the persistent stream configured as {@code name} is opened in the Axon Server
     * context of the given {@code tenant}.
     *
     * @param name   the configured name of the persistent stream
     * @param tenant the tenant whose stream is opened
     * @return the stream id to open the tenant's persistent stream under, never {@code null} or empty
     */
    String resolve(String name, TenantDescriptor tenant);
}
