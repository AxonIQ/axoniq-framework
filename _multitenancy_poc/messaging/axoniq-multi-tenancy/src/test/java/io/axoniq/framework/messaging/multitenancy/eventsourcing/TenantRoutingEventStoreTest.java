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

import io.axoniq.framework.messaging.multitenancy.api.NoSuchTenantException;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TenantRoutingEventStoreTest {

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant2");

    @Test
    void publishRoutesToResolvedTenant() {
        RecordingEventStore tenant1 = new RecordingEventStore();
        RecordingEventStore tenant2 = new RecordingEventStore();

        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenant -> tenant.tenantId().equals(TENANT_1.tenantId()) ? tenant1 : tenant2,
                alwaysTenant(TENANT_2)
        );

        testSubject.registerTenant(TENANT_1);
        testSubject.registerTenant(TENANT_2);

        EventMessage event = new GenericEventMessage(new MessageType("TestEvent"), "payload");
        testSubject.publish(null, List.of(event)).join();

        assertThat(tenant1.publishCount).isZero();
        assertThat(tenant2.publishCount).isEqualTo(1);
        assertThat(tenant2.lastEvents.getFirst()).isSameAs(event);
    }

    @Test
    void subscribeRegistersOnExistingTenants() {
        RecordingEventStore tenant1 = new RecordingEventStore();
        RecordingEventStore tenant2 = new RecordingEventStore();

        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenant -> tenant.tenantId().equals(TENANT_1.tenantId()) ? tenant1 : tenant2,
                alwaysTenant(TENANT_2)
        );

        testSubject.registerTenant(TENANT_1);
        testSubject.registerTenant(TENANT_2);

        Registration registration = testSubject.subscribe((events, context) -> CompletableFuture.completedFuture(null));

        assertThat(tenant1.subscribeCount).isEqualTo(1);
        assertThat(tenant2.subscribeCount).isEqualTo(1);
        assertThat(registration.cancel()).isTrue();
    }

    @Test
    void registerAndStartTenantSubscribesExistingConsumers() {
        RecordingEventStore tenant1 = new RecordingEventStore();

        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenant -> tenant1,
                alwaysTenant(TENANT_1)
        );

        testSubject.subscribe((events, context) -> CompletableFuture.completedFuture(null));
        testSubject.registerAndStartTenant(TENANT_1);

        assertThat(tenant1.subscribeCount).isEqualTo(1);
    }

    @Test
    void unregisterTenantRemovesTenantFromRouting() {
        RecordingEventStore tenant1 = new RecordingEventStore();
        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenant -> tenant1,
                alwaysTenant(TENANT_1)
        );

        Registration registration = testSubject.registerTenant(TENANT_1);
        assertThat(registration.cancel()).isTrue();

        EventMessage event = new GenericEventMessage(new MessageType("TestEvent"), "payload");
        assertThatThrownBy(() -> testSubject.publish(null, List.of(event)).join())
                .isInstanceOf(NoSuchTenantException.class);
    }

    @Test
    void tenantSegmentsReturnsRegisteredTenants() {
        RecordingEventStore tenant1 = new RecordingEventStore();
        RecordingEventStore tenant2 = new RecordingEventStore();

        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenant -> tenant.tenantId().equals(TENANT_1.tenantId()) ? tenant1 : tenant2,
                alwaysTenant(TENANT_2)
        );

        testSubject.registerTenant(TENANT_1);
        testSubject.registerTenant(TENANT_2);

        assertThat(testSubject.tenantSegments().keySet())
                .hasSize(2)
                .containsExactlyInAnyOrder(TENANT_1, TENANT_2);
    }

    @Test
    void unknownTenantThrowsException() {
        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenant -> new RecordingEventStore(),
                alwaysTenant(TENANT_1)
        );

        EventMessage event = new GenericEventMessage(new MessageType("TestEvent"), "payload");
        assertThatThrownBy(() -> testSubject.publish(null, List.of(event)).join())
                .isInstanceOf(NoSuchTenantException.class);
    }

    private static TenantResolver<Message> alwaysTenant(TenantDescriptor tenantDescriptor) {
        return (message, tenants) -> tenantDescriptor;
    }

    private static final class RecordingEventStore implements EventStore {

        private int publishCount;
        private int subscribeCount;
        private List<? extends EventMessage> lastEvents = List.of();
        private final List<BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>>> consumers =
                new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<Void> publish(ProcessingContext context, List<? extends EventMessage> events) {
            publishCount++;
            lastEvents = new ArrayList<>(events);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Registration subscribe(
                BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer) {
            subscribeCount++;
            consumers.add(eventsBatchConsumer);
            return () -> consumers.remove(eventsBatchConsumer);
        }

        @Override
        public MessageStream<EventMessage> open(StreamingCondition condition, ProcessingContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public EventStoreTransaction transaction(ProcessingContext processingContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken> firstToken(
                ProcessingContext context
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken> latestToken(
                ProcessingContext context
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken> tokenAt(
                Instant at,
                ProcessingContext context
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            // no-op
        }
    }
}
