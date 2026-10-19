/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.deadletter;

import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.framework.messaging.eventhandling.deadletter.SequencedDeadLetterQueueFactory;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.eventhandling.EventMessage;

import static java.util.Objects.requireNonNull;

/**
 * Framework-internal adapter from {@link TenantAwareSequencedDeadLetterQueueFactory} to the regular
 * {@link SequencedDeadLetterQueueFactory} required by event processing.
 * <p>
 * An event processor creates one dead-letter queue for a processing group through the regular factory contract. That
 * contract has no tenant argument, so it cannot create a tenant-specific queue directly. This adapter creates a
 * {@link TenantRoutingSequencedDeadLetterQueue} instead. For every operation, that queue resolves the tenant from the
 * processing context and asks the registry for the concrete queue created by the tenant-aware factory.
 * <p>
 * Applications register {@link TenantAwareSequencedDeadLetterQueueFactory}; the multi-tenancy configuration enhancer
 * creates this adapter automatically.
 *
 * @author Jan Galinski
 * @since 5.4.0
 */
class TenantRoutingSequencedDeadLetterQueueFactory implements SequencedDeadLetterQueueFactory {

    private final TenantAwareSequencedDeadLetterQueueFactory delegate;
    private final TenantRoutingSequencedDeadLetterQueueRegistry registry;

    TenantRoutingSequencedDeadLetterQueueFactory(TenantAwareSequencedDeadLetterQueueFactory delegate,
                                                 TenantRoutingSequencedDeadLetterQueueRegistry registry) {
        this.delegate = requireNonNull(delegate, "The delegate must not be null");
        this.registry = requireNonNull(registry, "The registry must not be null");
    }

    @Override
    public SequencedDeadLetterQueue<EventMessage> create(String processorName, Configuration configuration) {
        return new TenantRoutingSequencedDeadLetterQueue(processorName, configuration, delegate, registry);
    }
}
