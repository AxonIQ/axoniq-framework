/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.axonserver.eventsourcing;

import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.api.RecordingAxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngine;
import io.axoniq.framework.axonserver.connector.event.TaggedEventConverter;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SnapshotCapableEventStorageEngine;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.*;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AxonServerTenantEventStorageEngineFactoryTest {

    private final RecordingAxonServerConnectionManager connectionManager = new RecordingAxonServerConnectionManager();
    private final Configuration configuration =
            MessagingConfigurer.create()
                               .componentRegistry(registry -> registry.registerComponent(
                                       AxonServerConnectionManager.class, config -> connectionManager))
                               .build();
    private final AxonServerTenantEventStorageEngineFactory testSubject =
            new AxonServerTenantEventStorageEngineFactory(configuration);

    @BeforeEach
    void registerTenants() {
        testSubject.registerTenant(TENANT_A);
        testSubject.registerTenant(TENANT_B);
    }

    @Test
    void buildsAnAxonServerEngineAgainstTheTenantContext() {
        assertThat(testSubject.engineFor(TENANT_A)).isInstanceOf(AxonServerEventStorageEngine.class);
        assertThat(connectionManager.requestedContexts()).containsExactly(TENANT_A.tenantId());
    }

    // Combining a tenant's engine with that tenant's snapshot store is the routing engine's job, not this factory's, so
    // the engine arrives raw. MultiTenantEventStorageEngineTest covers the combining itself.
    @Test
    void buildsTheEngineWithoutDecoratingItWithASnapshotStore() {
        assertThat(testSubject.engineFor(TENANT_A)).isNotInstanceOf(SnapshotCapableEventStorageEngine.class);
    }

    @Test
    void usesTheConfiguredEventConverterWhenNoTenantConverterProviderIsRegistered() {
        EventConverter defaultConverter = configuration.getComponent(EventConverter.class);
        MockComponentDescriptor engineDescriptor = new MockComponentDescriptor();
        MockComponentDescriptor converterDescriptor = new MockComponentDescriptor();

        testSubject.engineFor(TENANT_A).describeTo(engineDescriptor);
        engineDescriptor.<TaggedEventConverter>getProperty("converter").describeTo(converterDescriptor);

        assertThat(converterDescriptor.<EventConverter>getProperty("converter")).isSameAs(defaultConverter);
    }

    @Test
    void cachesTheEnginePerTenant() {
        assertThat(testSubject.engineFor(TENANT_A)).isSameAs(testSubject.engineFor(TENANT_A));
        assertThat(connectionManager.requestedContexts()).containsExactly(TENANT_A.tenantId());
    }

    @Test
    void rejectsATenantThatIsNotRegistered() {
        TenantDescriptor unregistered = TenantDescriptor.tenantWithId("unregistered");

        assertThatThrownBy(() -> testSubject.engineFor(unregistered))
                .isInstanceOf(TenantNotResolvedException.class)
                .hasMessageContaining(unregistered.tenantId());
        assertThat(connectionManager.requestedContexts()).isEmpty();
    }

    // The connection manager disconnects a removed tenant's connection, so a cached engine bound to it must be dropped.
    @Test
    void aReAddedTenantGetsAFreshEngineAgainstANewConnection() {
        EventStorageEngine before = testSubject.engineFor(TENANT_A);

        assertThat(testSubject.registerTenant(TENANT_A).cancel()).isTrue();
        testSubject.registerTenant(TENANT_A);

        assertThat(testSubject.engineFor(TENANT_A)).isNotSameAs(before);
        assertThat(connectionManager.requestedContexts())
                .containsExactly(TENANT_A.tenantId(), TENANT_A.tenantId());
    }

    @Test
    void registerAndStartTenantFollowsTheSameLifecycleAsRegisterTenant() {
        EventStorageEngine before = testSubject.engineFor(TENANT_A);

        assertThat(testSubject.registerAndStartTenant(TENANT_A).cancel()).isTrue();
        testSubject.registerAndStartTenant(TENANT_A);

        assertThat(testSubject.engineFor(TENANT_A)).isNotSameAs(before);
    }
}
