/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.deadletter;

import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.eventhandling.EventMessage;

/**
 * Application-facing factory for creating the concrete {@link SequencedDeadLetterQueue} of one tenant.
 * <p>
 * The regular event-processing {@code SequencedDeadLetterQueueFactory} only receives a processing group and cannot
 * select tenant-specific infrastructure, such as a tenant's {@code DataSource}. Applications should register this
 * factory instead. The {@link TenantRoutingSequencedDeadLetterQueueRegistry} invokes it lazily once for every tenant
 * and processing group, then retains that concrete queue until the tenant is removed.
 * <p>
 * This factory creates the queue that stores dead letters; it does not route operations. Routing is provided by the
 * framework-internal {@link TenantRoutingSequencedDeadLetterQueueFactory}.
 *
 * @author Jan Galinski
 * @since 5.4.0
 */
@FunctionalInterface
public interface TenantAwareSequencedDeadLetterQueueFactory {

    /**
     * Creates the concrete dead-letter queue for the given {@code tenant} and {@code processorName}.
     *
     * @param tenant        the tenant for which the queue is created
     * @param processorName the processor for which the queue is created
     * @param configuration the configuration for component lookup
     * @return the concrete dead-letter queue for the given tenant and processing group
     */
    SequencedDeadLetterQueue<EventMessage> create(
            TenantDescriptor tenant,
            String processorName,
            Configuration configuration
    );
}
