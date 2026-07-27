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

package io.axoniq.framework.messaging.multitenancy.axonserver;

import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantSnapshotStoreFactory;
import io.axoniq.framework.messaging.multitenancy.util.RecordingAxonServerConnectionManager;
import io.axoniq.framework.messaging.multitenancy.util.RecordingSnapshotStore;
import io.axoniq.framework.messaging.multitenancy.util.TenantDescriptorMapping;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.*;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThat;

class AxonServerTenantEventStorageEngineFactoryTest {

    private final RecordingAxonServerConnectionManager connectionManager = new RecordingAxonServerConnectionManager();
    private final TenantDescriptorMapping<SnapshotStore> snapshotStores = new TenantDescriptorMapping<>();
    private final RecordingSnapshotStore snapshotStoreA = snapshotStores.entry(TENANT_A, new RecordingSnapshotStore());
    private final RecordingSnapshotStore snapshotStoreB = snapshotStores.entry(TENANT_B, new RecordingSnapshotStore());
    private final Configuration configuration =
            MessagingConfigurer.create()
                               .componentRegistry(registry -> registry
                                       .registerComponent(AxonServerConnectionManager.class,
                                                          config -> connectionManager)
                                       .registerComponent(TenantSnapshotStoreFactory.class,
                                                          config -> snapshotStores::apply))
                               .build();
    private final AxonServerTenantEventStorageEngineFactory testSubject =
            new AxonServerTenantEventStorageEngineFactory(configuration);

    @Test
    void buildsAnAxonServerEngineAgainstTheTenantContext() {
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        testSubject.engineFor(TENANT_A).describeTo(descriptor);

        Object complementedEngine = descriptor.getProperty("delegate");
        assertThat(complementedEngine).isInstanceOf(AxonServerEventStorageEngine.class);
        assertThat(connectionManager.requestedContexts()).containsExactly(TENANT_A.tenantId());
    }

    // Axon Server's engine is not its own snapshot store, so the tenant's engine is made snapshot capable with that
    // tenant's own store while it is built. Without that, snapshot sourcing would reach an engine that cannot resolve a
    // snapshot at all. The rule itself is covered by TenantEventStorageEngineFactoryTest.
    @Test
    void makesEachTenantEngineSnapshotCapableWithThatTenantsOwnStore() {
        MockComponentDescriptor tenantA = new MockComponentDescriptor();
        MockComponentDescriptor tenantB = new MockComponentDescriptor();

        testSubject.engineFor(TENANT_A).describeTo(tenantA);
        testSubject.engineFor(TENANT_B).describeTo(tenantB);

        Object storeForTenantA = tenantA.getProperty("snapshotStore");
        Object storeForTenantB = tenantB.getProperty("snapshotStore");
        assertThat(storeForTenantA).isSameAs(snapshotStoreA);
        assertThat(storeForTenantB).isSameAs(snapshotStoreB);
    }

    @Test
    void cachesTheEnginePerTenant() {
        assertThat(testSubject.engineFor(TENANT_A)).isSameAs(testSubject.engineFor(TENANT_A));
        assertThat(connectionManager.requestedContexts()).containsExactly(TENANT_A.tenantId());
    }

    // The connection manager disconnects a removed tenant's connection, so a cached engine bound to it must be dropped.
    @Test
    void aReAddedTenantGetsAFreshEngineAgainstANewConnection() {
        EventStorageEngine before = testSubject.engineFor(TENANT_A);

        assertThat(testSubject.registerTenant(TENANT_A).cancel()).isTrue();

        assertThat(testSubject.engineFor(TENANT_A)).isNotSameAs(before);
        assertThat(connectionManager.requestedContexts())
                .containsExactly(TENANT_A.tenantId(), TENANT_A.tenantId());
    }

    @Test
    void registerAndStartTenantFollowsTheSameLifecycleAsRegisterTenant() {
        EventStorageEngine before = testSubject.engineFor(TENANT_A);

        assertThat(testSubject.registerAndStartTenant(TENANT_A).cancel()).isTrue();

        assertThat(testSubject.engineFor(TENANT_A)).isNotSameAs(before);
    }
}
