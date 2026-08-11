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

package io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSource;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantScopedCache;
import io.axoniq.framework.messaging.multitenancy.configuration.TenantComponentProviderUtil;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A {@link SubscribableEventSource} consuming one persistent stream per tenant, labelling every event it delivers with
 * the tenant it came from.
 * <p>
 * Each tenant is an Axon Server context, and a persistent stream lives in one context, so this source holds one ordinary
 * {@link PersistentStreamEventSource} per tenant and fans a single consumer out to all of them. The stream carries the
 * configured name in every tenant's context, so a tenant's stream appears in Axon Server named exactly as configured.
 * <p>
 * This source follows the tenant lifecycle only while it has a subscriber. {@link #subscribe(BiFunction)} subscribes it
 * to the {@link TenantProvider} as a {@link MultiTenantAwareComponent}, which immediately replays the known tenants and
 * so opens a stream for each, while tenants added or removed later arrive through
 * {@link #registerAndStartTenant(TenantDescriptor)} and the cancellation of their registration. Cancelling the
 * subscription unsubscribes from the provider again, which closes every tenant's stream and releases its scheduler.
 * Binding the tenant subscription to the consumer's, rather than to a lifecycle phase, keeps the two in step: no stream
 * is ever open without a consumer to feed, and none stays open once the consumer is gone.
 * <p>
 * Marked {@link Internal} as concrete, internal implementation behind the
 * {@link MultiTenantPersistentStreamEventSourceFactory}.
 *
 * @author Jakob Hatzl
 * @see MultiTenantPersistentStreamEventSourceFactory
 * @since 5.3.0
 */
@Internal
public class MultiTenantPersistentStreamEventSource implements SubscribableEventSource, MultiTenantAwareComponent {

    private static final BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>>
            NO_OP_CONSUMER = (events, context) -> CompletableFuture.completedFuture(null);
    private static final Logger logger = LoggerFactory.getLogger(MultiTenantPersistentStreamEventSource.class);

    private final String name;
    private final PersistentStreamProperties properties;
    private final Function<String, ScheduledExecutorService> schedulerFactory;
    private final int batchSize;
    private final Configuration configuration;
    private final TenantProvider tenantProvider;

    private final TenantScopedCache<TenantStream> tenantStreams;

    // Written and read outside the monitor, on either side of the tenant provider callback, so it needs to be visible
    // across threads. Which thread gets to write it is already settled by the 'consumer' field: only the call that
    // claimed the subscription reaches the write, and only the call that releases it again reaches the read.
    private volatile @Nullable Registration tenantSubscription;

    // Written under 'this', together with the decision to open a tenant's stream, so a tenant arriving concurrently with
    // a subscription either opens its stream itself or is covered by the provider's replay, never both and never
    // neither. Volatile so describeTo can read it without contending with a subscription.
    private volatile BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> consumer =
            NO_OP_CONSUMER;

    /**
     * Constructs a {@code MultiTenantPersistentStreamEventSource} for the persistent stream with the given
     * {@code name}.
     *
     * @param name             the name of the persistent stream, used as the stream identifier in every tenant's Axon
     *                         Server context
     * @param properties       the properties applied when creating each tenant's persistent stream
     * @param schedulerFactory the factory creating a tenant's {@link ScheduledExecutorService} for the pool name given
     *                         to it, so each tenant's stream runs on threads of its own rather than sharing one pool
     * @param batchSize        the maximum number of events to deliver per batch
     * @param configuration    the configuration supplying the components each tenant's stream is built from
     * @param tenantProvider   the provider whose tenants this source opens a stream for while it has a subscriber
     * @throws NullPointerException     if any of the given arguments is {@code null}
     * @throws IllegalArgumentException if the given {@code name} is empty or {@code batchSize} is not positive
     */
    public MultiTenantPersistentStreamEventSource(String name,
                                                  PersistentStreamProperties properties,
                                                  Function<String, ScheduledExecutorService> schedulerFactory,
                                                  int batchSize,
                                                  Configuration configuration,
                                                  TenantProvider tenantProvider) {
        this.name = Objects.requireNonNull(name, "The name must not be null");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("The name must not be empty.");
        }
        this.properties = Objects.requireNonNull(properties, "The persistent stream properties must not be null");
        this.schedulerFactory = Objects.requireNonNull(schedulerFactory, "The scheduler factory must not be null");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("The batch size must be positive, but was: " + batchSize);
        }
        this.batchSize = batchSize;
        this.configuration = Objects.requireNonNull(configuration, "The configuration must not be null");
        this.tenantProvider = Objects.requireNonNull(tenantProvider, "The tenant provider must not be null");
        this.tenantStreams = new TenantScopedCache<>(this::createTenantStream,
                                                     this::releaseTenantStream,
                                                     "the multi-tenant persistent stream event source [" + name + "]");
    }

    /**
     * Subscribes the given {@code eventsBatchConsumer} to the persistent stream of every tenant, opening a stream for
     * each tenant known to the {@link TenantProvider} and for every tenant added while the subscription lasts.
     * <p>
     * Only one consumer can be subscribed at a time, matching the single-tenant behaviour: subscribing the same consumer
     * again is a no-op, while a different one is rejected. Cancelling the returned {@link Registration} closes every
     * tenant's stream and releases its scheduler, after which this source can be subscribed again.
     *
     * @param eventsBatchConsumer the consumer receiving batches of events, each with the tenant of its stream on the
     *                            {@link ProcessingContext}
     * @return a registration closing every tenant's stream on cancellation
     * @throws IllegalStateException if another consumer is already subscribed
     */
    @Override
    public Registration subscribe(
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer
    ) {
        Objects.requireNonNull(eventsBatchConsumer, "The events batch consumer must not be null");
        if (claimConsumer(eventsBatchConsumer)) {
            logger.debug("Subscribing to persistent stream [{}] across all tenants.", name);
            // Subscribed outside the monitor on purpose: the provider takes its own monitor and calls back into
            // registerTenant, which takes ours. Acquiring the provider's monitor while holding ours would invert the
            // lock order of that callback and of the removal path, so the two could deadlock.
            tenantSubscription = tenantProvider.subscribe(this);
        }
        return () -> unsubscribe(eventsBatchConsumer);
    }

    /**
     * Opens the persistent stream of the given {@code tenantDescriptor} and joins it to the subscribed consumer.
     * <p>
     * Called by the {@link TenantProvider} for each tenant known when this source subscribed to it. The streams of the
     * other tenants are untouched, so a tenant arriving here does not pause their processing.
     *
     * @param tenantDescriptor the tenant to register with this source
     * @return a registration closing the tenant's stream and releasing its scheduler on cancellation
     */
    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        return addTenant(tenantDescriptor);
    }

    /**
     * Behaves identically to {@link #registerTenant(TenantDescriptor)}, which already opens the tenant's stream: this
     * source is only registered with the {@link TenantProvider} while it has a consumer to feed, so there is no
     * registered-but-not-started state to distinguish.
     *
     * @param tenantDescriptor the tenant to register and start with this source
     * @return a registration closing the tenant's stream and releasing its scheduler on cancellation
     */
    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        return addTenant(tenantDescriptor);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("name", name);
        descriptor.describeProperty("subscribed", consumer != NO_OP_CONSUMER);
        tenantStreams.describeTo(descriptor);
    }

    // Returns true only for the call that established the subscription, so the tenant provider is subscribed once.
    private synchronized boolean claimConsumer(
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer
    ) {
        if (consumer == eventsBatchConsumer) {
            return false;
        }
        if (consumer != NO_OP_CONSUMER) {
            throw new IllegalStateException(String.format(
                    "%s: Cannot subscribe to MultiTenantPersistentStreamEventSource with another consumer: "
                            + "there is already an active subscription.", name));
        }
        consumer = eventsBatchConsumer;
        return true;
    }

    private boolean unsubscribe(
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer
    ) {
        synchronized (this) {
            if (consumer != eventsBatchConsumer) {
                return false;
            }
            consumer = NO_OP_CONSUMER;
        }
        // Cancelled outside the monitor, for the lock-order reason given in subscribe. Cancelling deregisters every
        // tenant registered on this source's behalf, so each tenantStream is evicted, closing its stream and scheduler.
        Registration subscription = tenantSubscription;
        tenantSubscription = null;
        if (subscription != null) {
            subscription.cancel();
        }
        return true;
    }

    // Registered outside the monitor, since the tenant only has to be known before its stream can open. Opening it is
    // then done under the monitor, so it cannot race a subscription into opening the stream twice or not at all.
    private Registration addTenant(TenantDescriptor tenantDescriptor) {
        Registration registration = tenantStreams.registerTenant(tenantDescriptor);
        synchronized (this) {
            if (consumer != NO_OP_CONSUMER) {
                // deliberately ignores the result, called only to force creation of the tenantStream
                tenantStreams.componentFor(tenantDescriptor);
            }
        }
        return registration;
    }

    // Runs inside the cache update, so the tenantStream is subscribed here rather than by the caller: whichever tenant
    // triggers the creation gets a stream that is already open and feeding the consumer.
    private TenantStream createTenantStream(TenantDescriptor tenant) {
        ScheduledExecutorService scheduler = schedulerFactory.apply(name + "@" + tenant.tenantId());
        PersistentStreamEventSource source = new PersistentStreamEventSource(
                name,
                configuration.getComponent(AxonServerConnectionManager.class),
                configuration.getComponent(AxonServerConfiguration.class),
                tenantEventConverter(tenant),
                configuration.getOptionalComponent(EventTypeResolver.class).orElse(EventTypeResolver.DEFAULT),
                properties,
                scheduler,
                configuration.getComponent(UnitOfWorkFactory.class),
                processingContext -> processingContext.withResource(TenantDescriptor.RESOURCE_KEY, tenant),
                batchSize,
                tenant.tenantId()
        );
        TenantStream tenantStream = new TenantStream(source, scheduler);
        tenantStream.subscribe(consumer);
        logger.debug("Opened persistent stream [{}] for tenant [{}].", name, tenant.tenantId());
        return tenantStream;
    }

    private EventConverter tenantEventConverter(TenantDescriptor tenant) {
        EventConverter defaultConverter = configuration.getComponent(EventConverter.class);
        return TenantComponentProviderUtil.find(configuration, Converter.class)
                                          .<EventConverter>map(provider -> new DelegatingEventConverter(
                                              provider.componentFor(tenant)))
                                          .orElse(defaultConverter);
    }

    private void releaseTenantStream(TenantDescriptor tenant, TenantStream tenantStream) {
        tenantStream.close();
        logger.debug("Closed persistent stream [{}] for tenant [{}].", name, tenant.tenantId());
    }

    /**
     * One tenant's share of this source: its event source, the scheduler that source runs on, and its current
     * subscription. Grouping them lets eviction release everything belonging to the tenant at once.
     */
    private static final class TenantStream {

        private final PersistentStreamEventSource eventSource;
        private final ScheduledExecutorService scheduler;

        // Guarded by 'this', so a subscription is never opened twice nor cancelled while being opened.
        private @Nullable Registration subscription;

        private TenantStream(PersistentStreamEventSource eventSource, ScheduledExecutorService scheduler) {
            this.eventSource = eventSource;
            this.scheduler = scheduler;
        }

        private synchronized void subscribe(
                BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> consumer
        ) {
            if (subscription == null) {
                subscription = eventSource.subscribe(consumer);
            }
        }

        private synchronized void unsubscribe() {
            Registration current = subscription;
            if (current != null) {
                subscription = null;
                current.cancel();
            }
        }

        private void close() {
            unsubscribe();
            // Shut down after the stream is closed, so no batch is left without the threads to finish it.
            scheduler.shutdown();
        }
    }
}
