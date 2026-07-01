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

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamMessageSource;
import org.axonframework.common.configuration.Configuration;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Factory for creating a {@link PersistentStreamMessageSource} for a specific {@link TenantDescriptor}.
 * <p>
 * The created message source consumes from a tenant-specific Axon Server context and can be used as the event source
 * for a projection or event processor segment.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
@FunctionalInterface
public interface TenantPersistentStreamMessageSourceFactory {

    /**
     * Builds a new tenant-specific {@link PersistentStreamMessageSource}.
     *
     * @param name                       The persistent stream name.
     * @param persistentStreamProperties The persistent stream properties.
     * @param scheduler                  The scheduler used for stream processing.
     * @param batchSize                  The maximum number of events per batch.
     * @param context                    The explicit Axon Server context, or {@code null} to use the tenant id.
     * @param configuration              The Axon configuration used to resolve shared infrastructure components.
     * @param tenantDescriptor           The tenant to build the stream source for.
     * @return A tenant-specific persistent stream message source.
     */
    PersistentStreamMessageSource build(String name,
                                        PersistentStreamProperties persistentStreamProperties,
                                        ScheduledExecutorService scheduler,
                                        int batchSize,
                                        String context,
                                        Configuration configuration,
                                        TenantDescriptor tenantDescriptor);
}
