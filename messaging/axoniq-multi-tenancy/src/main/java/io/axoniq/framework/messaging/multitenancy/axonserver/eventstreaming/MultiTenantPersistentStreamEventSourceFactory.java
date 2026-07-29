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
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSourceFactory;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.SubscribableEventSource;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A {@link PersistentStreamEventSourceFactory} building a {@link MultiTenantPersistentStreamEventSource}, so a
 * configured persistent stream is consumed from every tenant's Axon Server context rather than from one.
 * <p>
 * Replaces the default factory while multi-tenancy is active, which is all a Spring Boot application needs to make its
 * persistent-stream-backed event processors multi-tenant: streams stay configured under
 * {@code axon.axonserver.persistent-streams} exactly as they are without multi-tenancy.
 * <p>
 * Each tenant's stream gets a {@link ScheduledExecutorService} of its own, so the configured {@code thread-count} keeps
 * meaning "threads for this stream" and a tenant whose stream is retrying cannot occupy the threads the other tenants
 * need. Because that thread count is not part of the
 * {@link #build(String, PersistentStreamProperties, ScheduledExecutorService, int, Configuration)} contract, it is read
 * back from the {@link AxonServerConfiguration} settings the stream was configured with. The pre-built
 * {@code scheduler} that contract supplies is left unused: it is built for a single stream, while this source needs one
 * pool per tenant. A {@link java.util.concurrent.Executors#newScheduledThreadPool(int) scheduled thread pool} starts no
 * threads until work is submitted, so the unused pool costs nothing, and the caller that built it still owns its
 * shutdown.
 *
 * @author Jakob Hatzl
 * @see MultiTenantPersistentStreamEventSource
 * @since 5.3.0
 */
public class MultiTenantPersistentStreamEventSourceFactory implements PersistentStreamEventSourceFactory {

    @Override
    public SubscribableEventSource build(String name,
                                         PersistentStreamProperties properties,
                                         ScheduledExecutorService scheduler,
                                         int batchSize,
                                         Configuration configuration) {
        AxonServerConfiguration serverConfiguration = configuration.getComponent(AxonServerConfiguration.class);
        return new MultiTenantPersistentStreamEventSource(
                name,
                properties,
                configuration.getComponent(PersistentStreamScheduledExecutorBuilder.class,
                                           PersistentStreamScheduledExecutorBuilder::defaultFactory),
                threadCountFor(name, serverConfiguration),
                batchSize,
                configuration,
                configuration.getComponent(TenantProvider.class)
        );
    }

    /**
     * Resolves the thread count configured for the stream with the given {@code streamName}.
     * <p>
     * Streams are configured under a map key that doubles as the stream name unless
     * {@link AxonServerConfiguration.PersistentStreamSettings#getName()} overrides it, so both are matched, the same way
     * the stream's name was resolved when it was registered. A stream that is in neither, which is the case for an
     * automatically created one, takes the thread count of the auto-persistent-stream settings.
     *
     * @param streamName          the resolved name of the stream to find the thread count for
     * @param serverConfiguration the Axon Server configuration holding the persistent stream settings
     * @return the thread count configured for the given {@code streamName}
     */
    private static int threadCountFor(String streamName, AxonServerConfiguration serverConfiguration) {
        for (Map.Entry<String, AxonServerConfiguration.PersistentStreamSettings> entry
                : serverConfiguration.getPersistentStreams().entrySet()) {
            String configuredName = entry.getValue().getName() != null ? entry.getValue().getName() : entry.getKey();
            if (configuredName.equals(streamName)) {
                return entry.getValue().getThreadCount();
            }
        }
        return serverConfiguration.getAutoPersistentStreamsSettings().getThreadCount();
    }
}
