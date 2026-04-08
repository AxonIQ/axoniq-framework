/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package org.axonframework.axonserver.connector.event.axon;

import io.axoniq.axonserver.connector.event.PersistentStream;
import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.eventhandling.configuration.SubscribableEventSourceDefinition;
import org.axonframework.messaging.core.SubscribableEventSource;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Definition of a {@link PersistentStreamMessageSource}.
 * <p>
 * Used to create {@code PersistentStreamMessageSource} instances with a specific Axon
 * {@link Configuration configuration}.
 *
 * @author Marc Gathier
 * @since 4.10.0
 */
public class PersistentStreamEventSourceDefinition implements SubscribableEventSourceDefinition {

    private final String name;
    private final PersistentStreamProperties persistentStreamProperties;
    private final ScheduledExecutorService scheduler;
    private final int batchSize;
    private final String context;

    private final PersistentStreamMessageSourceFactory messageSourceFactory;

    /**
     * Instantiates a {@code PersistentStreamMessageSourceDefinition} instance based on the given parameters.
     *
     * @param name                       The name of the persistent stream. It's a unique identifier of the
     *                                   {@link PersistentStream} connection with Axon Sever. Usage of the same name
     *                                   will overwrite the existing connection.
     * @param persistentStreamProperties The properties to create te persistent stream.
     * @param scheduler                  Scheduler used for persistent stream operations.
     * @param batchSize                  The batch size for collecting events.
     * @param context                    The context in which this persistent stream exists (or needs to be created).
     */
    public PersistentStreamEventSourceDefinition(String name,
                                                 PersistentStreamProperties persistentStreamProperties,
                                                 ScheduledExecutorService scheduler,
                                                 int batchSize,
                                                 String context,
                                                 PersistentStreamMessageSourceFactory messageSourceFactory) {
        this.name = name;
        this.persistentStreamProperties = persistentStreamProperties;
        this.scheduler = scheduler;
        this.batchSize = batchSize;
        this.context = context;
        this.messageSourceFactory = messageSourceFactory;
    }

    @Override
    public SubscribableEventSource create(Configuration configuration) {
        return messageSourceFactory.build(name,
                                          persistentStreamProperties,
                                          scheduler,
                                          batchSize,
                                          context,
                                          configuration);
    }
}
