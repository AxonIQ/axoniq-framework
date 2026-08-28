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

package io.axoniq.framework.messaging.multitenancy.deadletter;

import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.framework.messaging.eventhandling.deadletter.SequencedDeadLetterQueueFactory;
import org.axonframework.common.annotation.Internal;
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
@Internal
class TenantRoutingSequencedDeadLetterQueueFactory implements SequencedDeadLetterQueueFactory {

    private final TenantAwareSequencedDeadLetterQueueFactory delegate;
    private final TenantRoutingSequencedDeadLetterQueueRegistry registry;

    /**
     * Creates the adapter used by the multi-tenancy configuration enhancer.
     *
     * @param delegate the factory creating the underlying tenant-specific queues
     * @param registry the registry owning tenant lifecycle and tenant-specific queues
     */
    public TenantRoutingSequencedDeadLetterQueueFactory(TenantAwareSequencedDeadLetterQueueFactory delegate,
                                                        TenantRoutingSequencedDeadLetterQueueRegistry registry) {
        this.delegate = requireNonNull(delegate, "The delegate must not be null");
        this.registry = requireNonNull(registry, "The registry must not be null");
    }

    @Override
    public SequencedDeadLetterQueue<EventMessage> create(String processorName, Configuration configuration) {
        return new TenantRoutingSequencedDeadLetterQueue(processorName, configuration, delegate, registry);
    }
}
