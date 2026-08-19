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

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Adapts a regular dead-letter queue factory into one that creates a tenant-routing queue for each handling component.
 *
 * @author Jan Galinski
 * @since 5.4.0
 */
@Internal
public class TenantRoutingSequencedDeadLetterQueueFactory implements SequencedDeadLetterQueueFactory {

    private final SequencedDeadLetterQueueFactory delegate;
    private final TenantRoutingSequencedDeadLetterQueueRegistry registry;

    /**
     * Creates a factory that routes each queue operation to the queue of the tenant in its processing context.
     *
     * @param delegate       the factory creating the underlying queues
     * @param registry       the registry-owning tenant lifecycle and tenant-specific queues
     */
    public TenantRoutingSequencedDeadLetterQueueFactory(SequencedDeadLetterQueueFactory delegate,
                                                         TenantRoutingSequencedDeadLetterQueueRegistry registry) {
        this.delegate = requireNonNull(delegate, "The delegate must not be null");
        this.registry = requireNonNull(registry, "The registry must not be null");
    }

    @Override
    public SequencedDeadLetterQueue<EventMessage> create(String processingGroup, Configuration configuration) {
        return new TenantRoutingSequencedDeadLetterQueue(processingGroup, configuration, delegate, registry);
    }
}
