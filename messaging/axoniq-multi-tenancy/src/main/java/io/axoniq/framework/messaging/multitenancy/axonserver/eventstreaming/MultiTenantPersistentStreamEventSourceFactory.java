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

package io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSourceFactory;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.SubscribableEventSource;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/**
 * A {@link PersistentStreamEventSourceFactory} building a {@link MultiTenantPersistentStreamEventSource}, so a
 * configured persistent stream is consumed from every tenant's Axon Server context rather than from one.
 * <p>
 * Replaces the default factory while multi-tenancy is active, which is all a Spring Boot application needs to make its
 * persistent-stream-backed event processors multi-tenant: streams stay configured under
 * {@code axon.axonserver.persistent-streams} exactly as they are without multi-tenancy.
 * <p>
 * Marked {@link Internal} as concrete, internal implementation of the {@link PersistentStreamEventSourceFactory}.
 *
 * @author Jakob Hatzl
 * @see MultiTenantPersistentStreamEventSource
 * @since 5.3.0
 */
@Internal
public class MultiTenantPersistentStreamEventSourceFactory implements PersistentStreamEventSourceFactory {

    /**
     * Builds the {@link SubscribableEventSource} consuming the persistent stream described by the given parameters,
     * taking the {@link ScheduledExecutorService} instances it needs from the given {@code schedulerFactory}.
     * <p>
     * The supplied {@link Configuration} provides access to all registered framework components, such as the
     * {@link io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager} and
     * {@link org.axonframework.messaging.eventhandling.conversion.EventConverter}, so implementations do not need to
     * receive those as constructor arguments.
     * <p>
     * A factory is handed a scheduler factory rather than a scheduler, so implementations can create pools under their
     * control. The requested pool name is used to name the pool's threads, so the source a thread belongs to is visible
     * in a thread dump; pass the stream name for a single stream and a name distinguishing them for several.
     * <p>
     * For the {@code MultiTenantPersistentStreamEventSourceFactory} each tenant's stream gets a
     * {@link ScheduledExecutorService} of its own, taken from the scheduler factory the contract supplies, so the
     * configured {@code thread-count} keeps meaning "threads for this stream" and a tenant whose stream is retrying
     * cannot occupy the threads the other tenants need. Pools are named after the stream and the tenant they belong to,
     * so a thread dump shows whose stream a thread is working on.
     *
     * @param name             the unique stream name on Axon Server
     * @param properties       the persistent stream properties (segment count, sequencing policy, filter, etc.)
     * @param schedulerFactory the factory creating a {@link ScheduledExecutorService} for the pool name given to it
     * @param batchSize        the maximum number of events to deliver per batch
     * @param configuration    the framework configuration from which additional components can be retrieved
     * @return a new {@link SubscribableEventSource} consuming the described persistent stream
     */
    @Override
    public SubscribableEventSource build(String name,
                                         PersistentStreamProperties properties,
                                         Function<String, ScheduledExecutorService> schedulerFactory,
                                         int batchSize,
                                         Configuration configuration) {
        return new MultiTenantPersistentStreamEventSource(
                name,
                properties,
                schedulerFactory,
                batchSize,
                configuration,
                configuration.getComponent(TenantProvider.class)
        );
    }

    /**
     * Always throws, since a single {@link ScheduledExecutorService} shared across every tenant would defeat the
     * per-tenant thread isolation this factory exists to provide.
     *
     * @throws AxonConfigurationException always, directing the caller to the {@link Function}-based {@code build}
     *                                    overload instead
     */
    @Override
    public SubscribableEventSource build(String name, PersistentStreamProperties properties,
                                         ScheduledExecutorService scheduler, int batchSize,
                                         Configuration configuration) {
        throw new AxonConfigurationException(
                "MultiTenantPersistentStreamEventSourceFactory requires a scheduler per tenant; build the source "
                        + "through the schedulerFactory-based build(...) overload instead of handing it a single "
                        + "ScheduledExecutorService.");
    }
}
