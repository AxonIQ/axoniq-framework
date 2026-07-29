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
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Default implementation of {@link PersistentStreamEventSourceFactory}, building one source per Axon Server stream.
 * <p>
 * Tracks all stream names that have been used to create a source and logs a warning when the same Axon Server stream
 * name is used more than once. Two sources sharing the same server-side stream name will join the same stream, which is
 * typically a misconfiguration.
 *
 * @author Jakob Hatzl
 * @see PersistentStreamEventSourceFactory
 * @since 5.2.0
 */
public class DefaultPersistentStreamEventSourceFactory implements PersistentStreamEventSourceFactory {

    /**
     * Default singleton instance for the {@link DefaultPersistentStreamEventSourceFactory}. Use this to ensure stream
     * name duplication warnings across all streams constructed with this.
     */
    public static final PersistentStreamEventSourceFactory INSTANCE = new DefaultPersistentStreamEventSourceFactory();
    private static final Logger logger = LoggerFactory.getLogger(DefaultPersistentStreamEventSourceFactory.class);

    private final Set<String> seenStreamNames = new CopyOnWriteArraySet<>();

    @Override
    public SubscribableEventSource build(String name,
                                         PersistentStreamProperties properties,
                                         ScheduledExecutorService scheduler,
                                         int batchSize,
                                         Configuration configuration) {
        if (!seenStreamNames.add(name)) {
            logger.warn("""
                                A persistent stream event source with stream name '{}' has already been created. \
                                Two sources sharing the same Axon Server stream name will join the same \
                                server-side stream, which may cause unexpected behavior.""", name);
        }
        return new PersistentStreamEventSource(
                name,
                configuration.getComponent(AxonServerConnectionManager.class),
                configuration.getComponent(AxonServerConfiguration.class),
                configuration.getComponent(EventConverter.class),
                configuration.getOptionalComponent(EventTypeResolver.class)
                             .orElse(EventTypeResolver.DEFAULT),
                properties,
                scheduler,
                configuration.getComponent(UnitOfWorkFactory.class),
                batchSize
        );
    }
}
