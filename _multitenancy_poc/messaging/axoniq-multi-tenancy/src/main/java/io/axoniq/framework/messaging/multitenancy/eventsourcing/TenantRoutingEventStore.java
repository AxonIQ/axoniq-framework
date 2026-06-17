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

package io.axoniq.framework.messaging.multitenancy.eventsourcing;

import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.NoSuchTenantException;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantEventSegmentFactory;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;

/**
 * Tenant aware implementation of the {@link EventStore}.
 * <p>
 * Tenant-specific {@code EventStore} segments are resolved from the {@link EventMessage#metadata() event's metadata}.
 * The {@link #open(StreamingCondition, ProcessingContext)} operation throws an
 * {@link UnsupportedOperationException} as multi-tenant streaming requires combining streams from all tenants,
 * which should be handled at a higher level.
 *
 * @see TenantResolver
 *
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @author Theo Emanuelsson
 * @author Jan Galinski
 * @since 4.6.0
 */
public class TenantRoutingEventStore implements EventStore, MultiTenantAwareComponent {

    /**
     * The order in which the {@link TenantRoutingEventStore} is applied as a decorator to the {@link EventStore}.
     * <p>
     * Uses an order HIGHER than {@code InterceptingEventStore} (which is at {@code Integer.MIN_VALUE + 50})
     * to ensure multi-tenant routing is the outermost layer. Interceptors (correlation data, etc.) are applied
     * per-tenant inside each tenant's event store segment, not on the outer multi-tenant store.
     */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 75;

    private final Map<TenantDescriptor, EventStore> tenantSegments = new ConcurrentHashMap<>();
    private final List<BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>>> eventsBatchConsumers =
            new CopyOnWriteArrayList<>();
    private final Map<TenantDescriptor, Registration> subscribeRegistrations = new ConcurrentHashMap<>();

    private final TenantEventSegmentFactory tenantSegmentFactory;
    private final TenantResolver<Message> tenantResolver;

    /**
     * Instantiate a TenantRoutingEventStore with the given {@code tenantSegmentFactory} and
     * {@code tenantResolver}.
     *
     * @param tenantSegmentFactory the factory to create tenant-specific {@link EventStore} segments
     * @param tenantResolver       the resolver to determine the target tenant from a message
     */
    public TenantRoutingEventStore(TenantEventSegmentFactory tenantSegmentFactory,
                                   TenantResolver<Message> tenantResolver) {
        this.tenantSegmentFactory = Objects.requireNonNull(tenantSegmentFactory,
                                                           "TenantEventSegmentFactory may not be null");
        this.tenantResolver = Objects.requireNonNull(tenantResolver,
                                                     "TenantResolver may not be null");
    }

    @Override
    public CompletableFuture<Void> publish(@Nullable ProcessingContext context, List<? extends EventMessage> events) {
        if (events.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        Message resolveFrom = context != null ? Message.fromContext(context) : null;
        if (resolveFrom == null) {
            resolveFrom = events.get(0);
        }
        if (resolveFrom == null) {
            throw new IllegalStateException(
                    "Cannot publish to multi-tenant EventStore: no message found in ProcessingContext and no events available."
            );
        }

        return resolveTenant(resolveFrom).publish(context, events);
    }

    @Override
    public Registration subscribe(
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer
    ) {
        eventsBatchConsumers.add(eventsBatchConsumer);

        tenantSegments.forEach((tenant, segment) ->
                subscribeRegistrations.computeIfAbsent(tenant, t -> segment.subscribe(eventsBatchConsumer)));

        return () -> {
            eventsBatchConsumers.remove(eventsBatchConsumer);
            return subscribeRegistrations.values().stream()
                                        .map(Registration::cancel)
                                        .reduce((prev, acc) -> prev && acc)
                                        .orElse(false);
        };
    }

    @Override
    public MessageStream<EventMessage> open(StreamingCondition condition,
                                            @Nullable ProcessingContext context) {
        throw new UnsupportedOperationException(
                "Multi-tenant event streaming is not directly supported. Use individual tenant segments."
        );
    }

    @Override
    public EventStoreTransaction transaction(ProcessingContext processingContext) {
        Message message = Message.fromContext(processingContext);
        if (message == null) {
            throw new IllegalStateException("Cannot resolve tenant for event store transaction without a message in context");
        }
        EventStore tenantEventStore = resolveTenant(message);
        return tenantEventStore.transaction(processingContext);
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken(@Nullable ProcessingContext context) {
        throw new UnsupportedOperationException(
                "Multi-tenant token operations are not directly supported. Use individual tenant segments."
        );
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken(@Nullable ProcessingContext context) {
        throw new UnsupportedOperationException(
                "Multi-tenant token operations are not directly supported. Use individual tenant segments."
        );
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at, @Nullable ProcessingContext context) {
        throw new UnsupportedOperationException(
                "Multi-tenant token operations are not directly supported. Use individual tenant segments."
        );
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tenantSegments", tenantSegments);
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        tenantSegments.computeIfAbsent(tenantDescriptor, tenantSegmentFactory::apply);
        return () -> unregisterTenant(tenantDescriptor) != null;
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        tenantSegments.computeIfAbsent(tenantDescriptor, tenant -> {
            EventStore tenantSegment = tenantSegmentFactory.apply(tenantDescriptor);
            eventsBatchConsumers.forEach(consumer -> subscribeRegistrations.computeIfAbsent(
                    tenantDescriptor,
                    ignored -> tenantSegment.subscribe(consumer)
            ));
            return tenantSegment;
        });

        return () -> unregisterTenant(tenantDescriptor) != null;
    }

    public Map<TenantDescriptor, EventStore> tenantSegments() {
        return Collections.unmodifiableMap(tenantSegments);
    }

    private EventStore unregisterTenant(TenantDescriptor tenantDescriptor) {
        Registration registration = subscribeRegistrations.remove(tenantDescriptor);
        if (registration != null) {
            registration.cancel();
        }
        return tenantSegments.remove(tenantDescriptor);
    }

    private EventStore resolveTenant(Message message) {
        TenantDescriptor tenantDescriptor = tenantResolver.resolveTenant(message, tenantSegments.keySet());
        EventStore tenantEventStore = tenantSegments.get(tenantDescriptor);
        if (tenantEventStore == null) {
            throw new NoSuchTenantException(tenantDescriptor.tenantId());
        }
        return tenantEventStore;
    }
}
