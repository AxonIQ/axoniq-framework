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

import io.axoniq.framework.axonserver.connector.snapshot.AxonServerSnapshotStore;
import io.axoniq.framework.messaging.multitenancy.util.RecordingAxonServerConnectionManager;
import org.axonframework.conversion.ChainingContentTypeConverter;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.junit.jupiter.api.*;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static org.assertj.core.api.Assertions.assertThat;

class AxonServerTenantSnapshotStoreFactoryTest {

    private final RecordingAxonServerConnectionManager connectionManager = new RecordingAxonServerConnectionManager();
    private final Converter converter = new ChainingContentTypeConverter();
    private final AxonServerTenantSnapshotStoreFactory testSubject =
            new AxonServerTenantSnapshotStoreFactory(connectionManager, converter);

    @Test
    void buildsAnAxonServerSnapshotStoreAgainstTheTenantConnection() {
        SnapshotStore store = testSubject.storeFor(TENANT_A);

        assertThat(store).isInstanceOf(AxonServerSnapshotStore.class);
        assertThat(connectionManager.requestedContexts()).containsExactly(TENANT_A.tenantId());
    }

    @Test
    void cachesTheStorePerTenant() {
        assertThat(testSubject.storeFor(TENANT_A)).isSameAs(testSubject.storeFor(TENANT_A));
        assertThat(connectionManager.requestedContexts()).containsExactly(TENANT_A.tenantId());
    }

    // The connection manager disconnects a removed tenant's connection, so a cached store bound to it must be dropped.
    @Test
    void aReAddedTenantGetsAFreshStoreAgainstANewConnection() {
        SnapshotStore before = testSubject.storeFor(TENANT_A);

        assertThat(testSubject.registerTenant(TENANT_A).cancel()).isTrue();

        assertThat(testSubject.storeFor(TENANT_A)).isNotSameAs(before);
        assertThat(connectionManager.requestedContexts())
                .containsExactly(TENANT_A.tenantId(), TENANT_A.tenantId());
    }

    @Test
    void registerAndStartTenantFollowsTheSameLifecycleAsRegisterTenant() {
        SnapshotStore before = testSubject.storeFor(TENANT_A);

        assertThat(testSubject.registerAndStartTenant(TENANT_A).cancel()).isTrue();

        assertThat(testSubject.storeFor(TENANT_A)).isNotSameAs(before);
    }
}
