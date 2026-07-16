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

import io.axoniq.framework.messaging.multitenancy.api.MultiTenancyApiUtils.OptionalTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptors;
import io.axoniq.framework.messaging.multitenancy.api.TenantEventSegmentFactory;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;

import static io.axoniq.framework.messaging.multitenancy.api.MultiTenancyApiUtils.tenantDescriptorOptional;
import static io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException.tenantNotResolved;
import static java.util.Objects.requireNonNull;

/**
 * Tenant aware implementation of the {@link EventStore}.
 * <p>
 * Tenant-specific {@code EventStore} segments are resolved from the {@link EventMessage#metadata() event's metadata}.
 * The {@link #open(StreamingCondition, ProcessingContext)} operation throws an {@link UnsupportedOperationException} as
 * multi-tenant streaming requires combining streams from all tenants, which should be handled at a higher level.
 *
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @author Theo Emanuelsson
 * @author Jan Galinski
 * @see TenantResolver
 * @since 5.3.0
 */
public class TenantRoutingEventStore implements EventStore, MultiTenantAwareComponent, TenantDescriptors {

    private final Map<TenantDescriptor, EventStore> tenantSegments = new ConcurrentHashMap<>();
    private final List<BiFunction<List<? extends EventMessage>, @Nullable ProcessingContext, CompletableFuture<?>>> eventsBatchConsumers =
            new CopyOnWriteArrayList<>();
    private final Map<TenantDescriptor, Registration> subscribeRegistrations = new ConcurrentHashMap<>();

    private final TenantEventSegmentFactory tenantSegmentFactory;
    private final OptionalTenantResolver optionalTenantResolver;

    /**
     * Instantiate a TenantRoutingEventStore with the given {@code tenantSegmentFactory} and {@code tenantResolver}.
     *
     * @param tenantSegmentFactory the factory to create tenant-specific {@link EventStore} segments
     * @param tenantResolver       the resolver to determine the target tenant from a message
     */
    public TenantRoutingEventStore(TenantEventSegmentFactory tenantSegmentFactory,
                                   TenantResolver tenantResolver) {
        this.tenantSegmentFactory = requireNonNull(tenantSegmentFactory, "TenantEventSegmentFactory may not be null");
        this.optionalTenantResolver = new OptionalTenantResolver(
                requireNonNull(tenantResolver, "TenantResolver may not be null"), this
        );
    }

    @Override
    public CompletableFuture<Void> publish(@Nullable ProcessingContext context, List<? extends EventMessage> events) {
        // no events to publish
        if (events.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // Two distinct use cases:
        Optional<TenantDescriptor> tenantDescriptor;

        // 1. ProcessingContext is not null -> triggered by a command, we can resolve the tenant from the context.
        //    it should be available as a context Resource, or in the command message metadata.
        if (context != null) {
            tenantDescriptor = tenantDescriptorOptional(context)
                    // TODO: debug the message on context should be the command message
                    .or(() -> optionalTenantResolver.apply(Message.fromContext(context)));
        }
        // 2. ProcessingContext is null -> triggered directly via EventBus, we should be able to resolve the tenant from the event message.
        else {
            tenantDescriptor = optionalTenantResolver.apply(events);
        }
        if (tenantDescriptor.isEmpty()) {
            throw tenantNotResolved("Tenant could not be resolved").get();
        }

        return eventStoreForTenant(tenantDescriptor.get()).publish(context, events);
    }

    @Override
    public Registration subscribe(
            BiFunction<List<? extends EventMessage>, @Nullable ProcessingContext, CompletableFuture<?>> eventsBatchConsumer
    ) {
        eventsBatchConsumers.add(eventsBatchConsumer);

        tenantSegments.forEach((tenant, segment) ->
                                       subscribeRegistrations.computeIfAbsent(tenant,
                                                                              t -> segment.subscribe(eventsBatchConsumer)));

        return () -> {
            eventsBatchConsumers.remove(eventsBatchConsumer);
            return subscribeRegistrations.values().stream()
                                         .map(Registration::cancel)
                                         .reduce((prev, acc) -> prev && acc)
                                         .orElse(false);
        };
    }

    @Override
    public EventStoreTransaction transaction(ProcessingContext processingContext) {
        TenantDescriptor tenantDescriptor = tenantDescriptorOptional(processingContext)
                .or(() -> optionalTenantResolver.apply(Message.fromContext(processingContext)))
                .orElseThrow(tenantNotResolved("Tenant could not be resolved"));
        return eventStoreForTenant(tenantDescriptor).transaction(processingContext);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tenantSegments", tenantSegments);
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        tenantSegments.computeIfAbsent(tenantDescriptor, tenantSegmentFactory);
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

    @Nullable
    private EventStore unregisterTenant(TenantDescriptor tenantDescriptor) {
        Registration registration = subscribeRegistrations.remove(tenantDescriptor);
        if (registration != null) {
            registration.cancel();
        }
        return tenantSegments.remove(tenantDescriptor);
    }

    private EventStore eventStoreForTenant(TenantDescriptor tenantDescriptor) {
        EventStore tenantEventStore = tenantSegments.get(tenantDescriptor);
        if (tenantEventStore == null) {
            throw TenantNotResolvedException.forTenantId(tenantDescriptor.tenantId());
        }
        return tenantEventStore;
    }

    @Override
    public List<TenantDescriptor> tenants() {
        return List.copyOf(tenantSegments.keySet());
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken(@Nullable ProcessingContext context) {
        throw multiTenantStreamingNotSupported();
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken(@Nullable ProcessingContext context) {
        throw multiTenantStreamingNotSupported();
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at, @Nullable ProcessingContext context) {
        throw multiTenantStreamingNotSupported();
    }

    @Override
    public MessageStream<EventMessage> open(StreamingCondition condition, @Nullable ProcessingContext context) {
        throw multiTenantStreamingNotSupported();
    }

    private static UnsupportedOperationException multiTenantStreamingNotSupported() {
        return new UnsupportedOperationException(
                "Multi-tenant event streaming is not directly supported. "
                        + "Use individual tenant segments."
        );
    }
}
