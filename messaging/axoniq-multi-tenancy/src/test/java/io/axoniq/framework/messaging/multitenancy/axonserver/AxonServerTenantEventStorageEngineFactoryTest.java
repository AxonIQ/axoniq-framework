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
import io.axoniq.framework.messaging.multitenancy.util.RecordingAxonServerConnectionManager;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.*;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static org.assertj.core.api.Assertions.assertThat;

class AxonServerTenantEventStorageEngineFactoryTest {

    private final RecordingAxonServerConnectionManager connectionManager = new RecordingAxonServerConnectionManager();
    private final Configuration configuration =
            MessagingConfigurer.create()
                               .componentRegistry(registry -> registry.registerComponent(
                                       AxonServerConnectionManager.class, config -> connectionManager))
                               .build();
    private final AxonServerTenantEventStorageEngineFactory testSubject =
            new AxonServerTenantEventStorageEngineFactory(configuration);

    @Test
    void buildsAnAxonServerEngineAgainstTheTenantContext() {
        EventStorageEngine engine = testSubject.engineFor(TENANT_A);

        assertThat(engine).isInstanceOf(AxonServerEventStorageEngine.class);
        assertThat(connectionManager.requestedContexts()).containsExactly(TENANT_A.tenantId());
    }

    @Test
    void cachesTheEnginePerTenant() {
        assertThat(testSubject.engineFor(TENANT_A)).isSameAs(testSubject.engineFor(TENANT_A));
        assertThat(connectionManager.requestedContexts()).containsExactly(TENANT_A.tenantId());
    }
}
