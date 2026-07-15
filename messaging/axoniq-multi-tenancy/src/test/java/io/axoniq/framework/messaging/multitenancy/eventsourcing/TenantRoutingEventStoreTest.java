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

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.util.TenantDescriptorMapping;
import org.axonframework.common.Registration;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.test.fixture.RecordingEventStore;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.alwaysTenant;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.recordingEventStore;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TenantRoutingEventStoreTest {

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant2");

    private final TenantDescriptorMapping<EventStore> tenantEventStores = new TenantDescriptorMapping<>();

    @Test
    void publishRoutesToResolvedTenant() {

        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenantEventStores::apply,
                alwaysTenant(TENANT_2)
        );

        RecordingEventStore tenant1 = tenantEventStores.entry(TENANT_1, recordingEventStore());
        RecordingEventStore tenant2 = tenantEventStores.entry(TENANT_2, recordingEventStore());

        testSubject.registerTenant(TENANT_1);
        testSubject.registerTenant(TENANT_2);

        EventMessage event = new GenericEventMessage(new MessageType("TestEvent"), "payload");
        testSubject.publish(null, List.of(event)).join();

        assertThat(tenant1.recorded()).isEmpty();
        assertThat(tenant2.recorded()).hasSize(1);
        assertThat(tenant2.recorded().getFirst()).isSameAs(event);
    }

    @Test
    void subscribeRegistersOnExistingTenants() {

        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenantEventStores::apply,
                alwaysTenant(TENANT_2)
        );

        tenantEventStores.entry(TENANT_1, recordingEventStore());
        tenantEventStores.entry(TENANT_2, recordingEventStore());

        testSubject.registerTenant(TENANT_1);
        testSubject.registerTenant(TENANT_2);

        Registration registration = testSubject.subscribe((events, context) -> CompletableFuture.completedFuture(null));

        assertThat(testSubject.tenants()).containsExactlyInAnyOrder(TENANT_1, TENANT_2);
        assertThat(registration.cancel()).isTrue();
    }

    @Test
    void registerAndStartTenantSubscribesExistingConsumers() {

        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenantEventStores::apply,
                alwaysTenant(TENANT_1)
        );

        tenantEventStores.entry(TENANT_1, recordingEventStore());

        testSubject.subscribe((events, context) -> CompletableFuture.completedFuture(null));
        testSubject.registerAndStartTenant(TENANT_1);

        assertThat(testSubject.tenants()).containsExactly(TENANT_1);
    }

    @Test
    void unregisterTenantRemovesTenantFromRouting() {
        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenantEventStores::apply,
                alwaysTenant(TENANT_1)
        );

        tenantEventStores.entry(TENANT_1, recordingEventStore());

        Registration registration = testSubject.registerTenant(TENANT_1);
        assertThat(registration.cancel()).isTrue();

        EventMessage event = new GenericEventMessage(new MessageType("TestEvent"), "payload");
        assertThatThrownBy(() -> testSubject.publish(null, List.of(event)).join())
                .isInstanceOf(TenantNotResolvedException.class);
    }

    @Test
    void tenantSegmentsReturnsRegisteredTenants() {
        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenantEventStores::apply,
                alwaysTenant(TENANT_2)
        );

        tenantEventStores.entry(TENANT_1, recordingEventStore());
        tenantEventStores.entry(TENANT_2, recordingEventStore());

        testSubject.registerTenant(TENANT_1);
        testSubject.registerTenant(TENANT_2);

        assertThat(testSubject.tenants())
                .hasSize(2)
                .containsExactlyInAnyOrder(TENANT_1, TENANT_2);
    }

    @Test
    void unknownTenantThrowsException() {

        TenantRoutingEventStore testSubject = new TenantRoutingEventStore(
                tenantEventStores::apply,
                alwaysTenant(TENANT_1)
        );

        EventMessage event = new GenericEventMessage(new MessageType("TestEvent"), "payload");
        assertThatThrownBy(() -> testSubject.publish(null, List.of(event)).join())
                .isInstanceOf(TenantNotResolvedException.class);
    }
}
