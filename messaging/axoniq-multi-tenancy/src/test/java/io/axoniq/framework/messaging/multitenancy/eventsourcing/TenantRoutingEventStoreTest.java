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

import io.axoniq.framework.messaging.multitenancy.api.MultiTenancyApiUtils;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.util.TenantDescriptorMapping;
import org.axonframework.common.Registration;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.test.fixture.RecordingEventStore;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TenantRoutingEventStoreTest {

    private final TenantDescriptorMapping<EventStore> tenantEventStores = new TenantDescriptorMapping<>();

    @Test
    void publishRoutesToResolvedTenant() {

        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenantEventStores::apply,
                alwaysTenant(TENANT_A)
        );

        RecordingEventStore tenantA = tenantEventStores.entry(TENANT_A, recordingEventStore());
        RecordingEventStore tenantB = tenantEventStores.entry(TENANT_B, recordingEventStore());

        testSubject.registerTenant(TENANT_A);
        testSubject.registerTenant(TENANT_B);

        EventMessage event = new GenericEventMessage(new MessageType("TestEvent"), "payload");
        testSubject.publish(null, List.of(event)).join();

        assertThat(tenantA.recorded()).hasSize(1);
        assertThat(tenantA.recorded().getFirst()).isSameAs(event);
        assertThat(tenantB.recorded()).isEmpty();
    }

    @Test
    void subscribeRegistersOnExistingTenants() {

        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenantEventStores::apply,
                alwaysTenant(TENANT_B)
        );

        tenantEventStores.entry(TENANT_A, recordingEventStore());
        tenantEventStores.entry(TENANT_B, recordingEventStore());

        testSubject.registerTenant(TENANT_A);
        testSubject.registerTenant(TENANT_B);

        Registration registration = testSubject.subscribe((events, context) -> CompletableFuture.completedFuture(null));

        assertThat(testSubject.tenants()).containsExactlyInAnyOrder(TENANT_A, TENANT_B);
        assertThat(registration.cancel()).isTrue();
    }

    @Test
    void registerAndStartTenantSubscribesExistingConsumers() {

        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenantEventStores::apply,
                alwaysTenant(TENANT_A)
        );

        tenantEventStores.entry(TENANT_A, recordingEventStore());

        testSubject.subscribe((events, context) -> CompletableFuture.completedFuture(null));
        testSubject.registerAndStartTenant(TENANT_A);

        assertThat(testSubject.tenants()).containsExactly(TENANT_A);
    }

    @Test
    void unregisterTenantRemovesTenantFromRouting() {
        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenantEventStores::apply,
                alwaysTenant(TENANT_A)
        );

        tenantEventStores.entry(TENANT_A, recordingEventStore());

        Registration registration = testSubject.registerTenant(TENANT_A);
        assertThat(registration.cancel()).isTrue();

        EventMessage event = new GenericEventMessage(new MessageType("TestEvent"), "payload");
        assertThatThrownBy(() -> testSubject.publish(null, List.of(event)).join())
                .isInstanceOf(TenantNotResolvedException.class);
    }

    @Test
    void tenantSegmentsReturnsRegisteredTenants() {
        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenantEventStores::apply,
                alwaysTenant(TENANT_B)
        );

        tenantEventStores.entry(TENANT_A, recordingEventStore());
        tenantEventStores.entry(TENANT_B, recordingEventStore());

        testSubject.registerTenant(TENANT_A);
        testSubject.registerTenant(TENANT_B);

        assertThat(testSubject.tenants())
                .hasSize(2)
                .containsExactlyInAnyOrder(TENANT_A, TENANT_B);
    }

    @Test
    void publishRoutesToTenantDescriptorResourceOnContext() {

        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenantEventStores::apply,
                alwaysTenant(TENANT_B)  // resolver always returns TENANT_B
        );

        RecordingEventStore tenantA = tenantEventStores.entry(TENANT_A, recordingEventStore());
        RecordingEventStore tenantB = tenantEventStores.entry(TENANT_B, recordingEventStore());

        testSubject.registerTenant(TENANT_A);
        testSubject.registerTenant(TENANT_B);

        // context carries TENANT_A as resource — should override the resolver (which always returns TENANT_B)
        StubProcessingContext context = new StubProcessingContext();
        context.withResource(MultiTenancyApiUtils.TENANT_RESOURCE_KEY, TENANT_A);

        EventMessage event = new GenericEventMessage(new MessageType("TestEvent"), "payload");
        testSubject.publish(context, List.of(event)).join();
        context.moveToPhase(ProcessingLifecycle.DefaultPhases.AFTER_COMMIT).join();

        assertThat(tenantA.recorded()).hasSize(1);
        assertThat(tenantA.recorded().getFirst()).isSameAs(event);
        assertThat(tenantB.recorded()).isEmpty();
    }

    @Test
    void unknownTenantThrowsException() {

        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenantEventStores::apply,
                alwaysTenant(TENANT_A)
        );

        EventMessage event = new GenericEventMessage(new MessageType("TestEvent"), "payload");
        assertThatThrownBy(() -> testSubject.publish(null, List.of(event)).join())
                .isInstanceOf(TenantNotResolvedException.class);
    }
}
