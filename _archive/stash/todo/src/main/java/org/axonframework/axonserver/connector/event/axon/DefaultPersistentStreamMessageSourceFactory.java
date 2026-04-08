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

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import org.axonframework.common.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Default implementation of the {@link PersistentStreamMessageSourceFactory} that creates
 * {@link PersistentStreamMessageSource} instances. Maintains a record of used stream names and provides warning logs
 * when name conflicts occur.
 *
 * @author Mateusz Nowak
 * @since 4.11
 */
public class DefaultPersistentStreamMessageSourceFactory implements PersistentStreamMessageSourceFactory {

    private static final Logger logger = LoggerFactory.getLogger(DefaultPersistentStreamMessageSourceFactory.class);
    private final Set<String> usedNames = new CopyOnWriteArraySet<>();

    /**
     * Creates a new {@code PersistentStreamMessageSource}. This method tracks stream names and logs warnings when name
     * conflicts are detected.
     *
     * @param name                       The name of the persistent stream. It's a unique identifier of the
     *                                   PersistentStream connection with Axon Sever. Usage of the same name will
     *                                   overwrite the existing connection.
     * @param persistentStreamProperties The properties to create te persistent stream.
     * @param scheduler                  Scheduler used for persistent stream operations.
     * @param batchSize                  The batch size for collecting events.
     * @param context                    The context in which this persistent stream exists (or needs to be created).
     * @param configuration              Global configuration of Axon components.
     * @return A new {@link PersistentStreamMessageSource} instance.
     */
    @Override
    public PersistentStreamMessageSource build(String name,
                                               PersistentStreamProperties persistentStreamProperties,
                                               ScheduledExecutorService scheduler,
                                               int batchSize,
                                               String context,
                                               Configuration configuration
    ) {
        if (!usedNames.add(name)) {
            logger.warn(
                    "A Persistent Stream connection with Axon Server is uniquely identified based on the name. Another Persistent Stream is started for a given name [{}]. The new connection will overwrite the existing connection.",
                    name);
        }
        return new PersistentStreamMessageSource(
                name, configuration, persistentStreamProperties, scheduler, batchSize, context
        );
    }
}
