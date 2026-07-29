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

package io.axoniq.framework.axonserver.connector.event;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.SubscribableEventSource;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Factory for creating the {@link SubscribableEventSource} that consumes a persistent stream on Axon Server.
 * <p>
 * Provides the customization point for persistent stream event source construction. The default implementation is
 * {@link DefaultPersistentStreamEventSourceFactory}, which builds a {@link PersistentStreamEventSource} and
 * additionally tracks stream names, emitting a warning when the same Axon Server stream name is used more than once.
 * <p>
 * In a Spring Boot application the default factory is registered as a {@code @ConditionalOnMissingBean}, so advanced
 * use cases can replace it by declaring their own bean of this type.
 *
 * @author Jakob Hatzl
 * @see DefaultPersistentStreamEventSourceFactory
 * @since 5.2.0
 */
@FunctionalInterface
public interface PersistentStreamEventSourceFactory {

    /**
     * Builds the {@link SubscribableEventSource} consuming the persistent stream described by the given parameters.
     * <p>
     * The supplied {@link Configuration} provides access to all registered framework components, such as the
     * {@link io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager} and
     * {@link org.axonframework.messaging.eventhandling.conversion.EventConverter}, so implementations do not need to
     * receive those as constructor arguments.
     *
     * @param name          the unique stream name on Axon Server
     * @param properties    the persistent stream properties (segment count, sequencing policy, filter, etc.)
     * @param scheduler     the scheduled executor to use for this stream's background tasks
     * @param batchSize     the maximum number of events to deliver per batch
     * @param configuration the framework configuration from which additional components can be retrieved
     * @return a new {@link SubscribableEventSource} consuming the described persistent stream
     */
    SubscribableEventSource build(String name,
                                  PersistentStreamProperties properties,
                                  ScheduledExecutorService scheduler,
                                  int batchSize,
                                  Configuration configuration);

    /**
     * The default {@link PersistentStreamEventSourceFactory} (a {@link DefaultPersistentStreamEventSourceFactory}) to
     * be used to create the {@link SubscribableEventSource} consuming a persistent stream.
     *
     * @return the default {@link PersistentStreamEventSourceFactory}
     */
    static PersistentStreamEventSourceFactory defaultFactory() {
        // by using the static singleton instance we can catch duplicate stream names in case streams are
        // constructed declaratively via the default instance in parallel
        return DefaultPersistentStreamEventSourceFactory.INSTANCE;
    }
}
