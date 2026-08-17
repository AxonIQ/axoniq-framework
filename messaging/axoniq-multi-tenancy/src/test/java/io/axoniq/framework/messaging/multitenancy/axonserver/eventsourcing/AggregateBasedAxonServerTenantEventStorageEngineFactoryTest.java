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

package io.axoniq.framework.messaging.multitenancy.axonserver.eventsourcing;

import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.api.RecordingAxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.AggregateBasedAxonServerEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.eventsourcing.eventstore.SnapshotCapableEventStorageEngine;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AggregateBasedAxonServerTenantEventStorageEngineFactoryTest {

    private final RecordingAxonServerConnectionManager connectionManager = new RecordingAxonServerConnectionManager();
    private final Configuration configuration =
            MessagingConfigurer.create()
                               .componentRegistry(registry -> registry.registerComponent(
                                       AxonServerConnectionManager.class, config -> connectionManager))
                               .build();
    private final AggregateBasedAxonServerTenantEventStorageEngineFactory testSubject =
            new AggregateBasedAxonServerTenantEventStorageEngineFactory(
                    connectionManager,
                    configuration.getComponent(EventConverter.class),
                    EventTypeResolver.DEFAULT
            );

    @BeforeEach
    void registerTenants() {
        testSubject.registerTenant(TENANT_A);
        testSubject.registerTenant(TENANT_B);
    }

    @Test
    void buildsAnAggregateBasedAxonServerEngineAgainstTheTenantContext() {
        assertThat(testSubject.engineFor(TENANT_A)).isInstanceOf(AggregateBasedAxonServerEventStorageEngine.class);
        assertThat(connectionManager.requestedContexts()).containsExactly(TENANT_A.tenantId());
    }

    @Test
    void buildsTheEngineWithoutDecoratingItWithASnapshotStore() {
        assertThat(testSubject.engineFor(TENANT_A)).isNotInstanceOf(SnapshotCapableEventStorageEngine.class);
    }

    @Test
    void usesTheConfiguredEventConverterWhenNoTenantConverterProviderIsRegistered() {
        EventConverter defaultConverter = configuration.getComponent(EventConverter.class);
        MockComponentDescriptor engineDescriptor = new MockComponentDescriptor();

        testSubject.engineFor(TENANT_A).describeTo(engineDescriptor);

        assertThat(engineDescriptor.<EventConverter>getProperty("converter")).isSameAs(defaultConverter);
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
