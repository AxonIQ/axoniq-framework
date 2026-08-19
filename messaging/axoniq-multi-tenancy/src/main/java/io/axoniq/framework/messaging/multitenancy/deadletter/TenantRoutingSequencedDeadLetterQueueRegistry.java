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
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantScopedCache;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Owns the tenant lifecycle and tenant-specific dead-letter queues created by routing queue factories.
 *
 * @author Jan Galinski
 * @since 5.4.0
 */
@Internal
public class TenantRoutingSequencedDeadLetterQueueRegistry implements MultiTenantAwareComponent {

    private final Map<QueueKey, TenantScopedCache<SequencedDeadLetterQueue<EventMessage>>> queues = new HashMap<>();
    private final Map<TenantDescriptor, TenantRegistration> tenants = new HashMap<>();

    /**
     * Returns the queue of the given tenant for the dead-letter queue identified by the supplied processing group,
     * configuration, and delegate factory.
     * <p>
     * The queue cache is created lazily and registered with every tenant known to this registry before it is used.
     * Synchronization keeps cache creation atomic with tenant registration and removal, so a new cache cannot escape a
     * concurrent tenant removal.
     *
     * @param processingGroup the processing group owning the queue
     * @param configuration   the configuration passed to the delegate factory
     * @param factory         the factory creating a tenant's underlying queue
     * @param tenant          the registered tenant whose queue to return
     * @return the tenant's queue
     */
    public synchronized SequencedDeadLetterQueue<EventMessage> queueFor(String processingGroup,
                                                                          Configuration configuration,
                                                                          SequencedDeadLetterQueueFactory factory,
                                                                          TenantDescriptor tenant) {
        QueueKey key = new QueueKey(processingGroup, configuration, factory);
        TenantScopedCache<SequencedDeadLetterQueue<EventMessage>> queueCache = queues.computeIfAbsent(
                key,
                ignored -> registerKnownTenants(new TenantScopedCache<>(
                        descriptor -> factory.create(processingGroup, configuration),
                        "the tenant-routing dead-letter queue [" + processingGroup + "]"
                ))
        );
        return queueCache.componentFor(tenant);
    }

    @Override
    public synchronized Registration registerTenant(TenantDescriptor tenantDescriptor) {
        requireNonNull(tenantDescriptor, "The tenant descriptor must not be null");
        TenantRegistration registration = new TenantRegistration(tenantDescriptor);
        TenantRegistration superseded = tenants.put(tenantDescriptor, registration);
        if (superseded != null) {
            superseded.cancel();
        }
        queues.values().forEach(registration::register);
        return () -> unregister(tenantDescriptor, registration);
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        return registerTenant(tenantDescriptor);
    }

    @Override
    public synchronized void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tenants", tenants.keySet());
    }

    private TenantScopedCache<SequencedDeadLetterQueue<EventMessage>> registerKnownTenants(
            TenantScopedCache<SequencedDeadLetterQueue<EventMessage>> queueCache) {
        tenants.values().forEach(registration -> registration.register(queueCache));
        return queueCache;
    }

    private synchronized boolean unregister(TenantDescriptor tenant, TenantRegistration registration) {
        if (!tenants.remove(tenant, registration)) {
            return false;
        }
        return registration.cancel();
    }

    private static final class TenantRegistration {

        private final TenantDescriptor tenant;
        private final Map<TenantScopedCache<SequencedDeadLetterQueue<EventMessage>>, Registration> registrations =
                new HashMap<>();

        private TenantRegistration(TenantDescriptor tenant) {
            this.tenant = tenant;
        }

        private void register(TenantScopedCache<SequencedDeadLetterQueue<EventMessage>> queueCache) {
            registrations.put(queueCache, queueCache.registerTenant(tenant));
        }

        private boolean cancel() {
            boolean cancelled = false;
            for (Registration registration : registrations.values()) {
                cancelled |= registration.cancel();
            }
            registrations.clear();
            return cancelled;
        }
    }

    private record QueueKey(String processingGroup,
                            Configuration configuration,
                            SequencedDeadLetterQueueFactory factory) {
    }
}
