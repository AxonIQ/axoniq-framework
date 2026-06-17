/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
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
package io.axoniq.framework.messaging.multitenancy.eventstreaming;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamMessageSource;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantPersistentStreamMessageSourceFactory;
import org.axonframework.common.Registration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * Multi-tenant {@link SubscribableEventSource} backed by tenant-specific persistent stream message sources.
 * <p>
 * Each tenant gets its own {@link PersistentStreamMessageSource} instance. The outer message source fans out a single
 * consumer to all tenant segments and keeps the tenant segments registered for later tenant additions.
 *
 * @author Jan Galinski
 * @since 5.2.0
 */
public class MultiTenantPersistentStreamMessageSource implements SubscribableEventSource, MultiTenantAwareComponent {

    private static final BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>>
            NO_OP_CONSUMER = (events, ctx) -> CompletableFuture.completedFuture(null);

    private final String name;
    private final TenantPersistentStreamMessageSourceFactory tenantPersistentStreamMessageSourceFactory;
    private final PersistentStreamProperties persistentStreamProperties;
    private final ScheduledExecutorService scheduler;
    private final int batchSize;
    private final String context;
    private final Configuration configuration;

    private final Map<TenantDescriptor, PersistentStreamMessageSource> tenantSegments = new ConcurrentHashMap<>();
    private final Map<TenantDescriptor, Registration> tenantRegistrations = new ConcurrentHashMap<>();
    private final AtomicReference<BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>>> consumer =
            new AtomicReference<>(NO_OP_CONSUMER);

    /**
     * Creates a new multi-tenant persistent stream message source.
     *
     * @param name                                    The persistent stream name.
     * @param persistentStreamProperties              The persistent stream properties.
     * @param scheduler                               The scheduler used for persistent stream processing.
     * @param batchSize                               The maximum number of events per batch.
     * @param context                                 The explicit Axon Server context, or {@code null} to derive it
     *                                                from the tenant.
     * @param configuration                           The Axon configuration.
     * @param tenantPersistentStreamMessageSourceFactory Factory for tenant-specific stream sources.
     */
    public MultiTenantPersistentStreamMessageSource(String name,
                                                    PersistentStreamProperties persistentStreamProperties,
                                                    ScheduledExecutorService scheduler,
                                                    int batchSize,
                                                    @Nullable String context,
                                                    Configuration configuration,
                                                    TenantPersistentStreamMessageSourceFactory tenantPersistentStreamMessageSourceFactory) {
        this.name = Objects.requireNonNull(name, "name may not be null");
        this.persistentStreamProperties = Objects.requireNonNull(persistentStreamProperties,
                                                                 "persistentStreamProperties may not be null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler may not be null");
        this.batchSize = batchSize;
        this.context = context;
        this.configuration = Objects.requireNonNull(configuration, "configuration may not be null");
        this.tenantPersistentStreamMessageSourceFactory = Objects.requireNonNull(
                tenantPersistentStreamMessageSourceFactory,
                "tenantPersistentStreamMessageSourceFactory may not be null"
        );
    }

    @Override
    public Registration subscribe(
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer
    ) {
        Objects.requireNonNull(eventsBatchConsumer, "eventsBatchConsumer may not be null");
        synchronized (this) {
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> current = consumer.get();
            if (current == NO_OP_CONSUMER) {
                consumer.set(eventsBatchConsumer);
                tenantSegments.forEach((tenant, segment) -> subscribeTenantSegment(tenant, segment, eventsBatchConsumer));
            } else if (current != eventsBatchConsumer) {
                throw new IllegalStateException(
                        String.format(
                                "%s: Cannot subscribe to MultiTenantPersistentStreamMessageSource with another consumer:"
                                        + " there is already an active subscription.",
                                name
                        )
                );
            }
        }
        return () -> {
            synchronized (MultiTenantPersistentStreamMessageSource.this) {
                if (consumer.get() == eventsBatchConsumer) {
                    tenantRegistrations.values().forEach(Registration::cancel);
                    tenantRegistrations.clear();
                    consumer.set(NO_OP_CONSUMER);
                }
                return true;
            }
        };
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        tenantSegments.computeIfAbsent(tenantDescriptor, this::createTenantSegment);
        return () -> unregisterTenant(tenantDescriptor) != null;
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        PersistentStreamMessageSource tenantSegment = tenantSegments.computeIfAbsent(tenantDescriptor,
                                                                                     this::createTenantSegment);
        BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> activeConsumer = consumer.get();
        if (activeConsumer != NO_OP_CONSUMER) {
            subscribeTenantSegment(tenantDescriptor, tenantSegment, activeConsumer);
        }
        return () -> unregisterTenant(tenantDescriptor) != null;
    }

    /**
     * Returns the registered tenant-specific message sources.
     *
     * @return An immutable view of the registered tenant segments.
     */
    public Map<TenantDescriptor, PersistentStreamMessageSource> tenantSegments() {
        return Collections.unmodifiableMap(tenantSegments);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("name", name);
        descriptor.describeProperty("tenantSegments", tenantSegments);
    }

    private PersistentStreamMessageSource createTenantSegment(TenantDescriptor tenantDescriptor) {
        String tenantContext = context == null || context.isBlank() ? tenantDescriptor.tenantId() : context;
        return tenantPersistentStreamMessageSourceFactory.build(
                name + "@" + tenantDescriptor.tenantId(),
                persistentStreamProperties,
                scheduler,
                batchSize,
                tenantContext,
                configuration,
                tenantDescriptor
        );
    }

    private void subscribeTenantSegment(
            TenantDescriptor tenantDescriptor,
            PersistentStreamMessageSource tenantSegment,
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer
    ) {
        tenantRegistrations.computeIfAbsent(tenantDescriptor, ignored -> tenantSegment.subscribe(eventsBatchConsumer));
    }

    private Registration unregisterTenant(TenantDescriptor tenantDescriptor) {
        Registration registration = tenantRegistrations.remove(tenantDescriptor);
        if (registration != null) {
            registration.cancel();
        }
        tenantSegments.remove(tenantDescriptor);
        return registration;
    }
}
