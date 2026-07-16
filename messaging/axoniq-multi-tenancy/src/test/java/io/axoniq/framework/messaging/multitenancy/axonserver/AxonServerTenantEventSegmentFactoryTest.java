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

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.*;
import org.mockito.junit.jupiter.*;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AxonServerTenantEventSegmentFactoryTest {

    @Mock
    private AxonServerConnectionManager connectionManager;

    @Mock
    private AxonServerConnection connection;

    @Mock
    private EventConverter eventConverter;

    @Mock
    private SimpleEventBus localSegment;

    @Mock
    private TagResolver tagResolver;

    private AxonServerTenantEventSegmentFactory testSubject() {
        return new AxonServerTenantEventSegmentFactory(connectionManager, eventConverter, localSegment, tagResolver);
    }

    @Nested
    class ApplyingATenant {

        @Test
        void returnsAStorageEngineBackedEventStore() {
            when(connectionManager.getConnection(TENANT_A.tenantId())).thenReturn(connection);

            EventStore result = testSubject().apply(TENANT_A);

            assertThat(result).isInstanceOf(StorageEngineBackedEventStore.class);
        }

        @Test
        void usesTheTenantIdAsConnectionContext() {
            when(connectionManager.getConnection(TENANT_A.tenantId())).thenReturn(connection);

            testSubject().apply(TENANT_A);

            verify(connectionManager).getConnection(TENANT_A.tenantId());
        }

        @Test
        void returnsDifferentEventStoresForDifferentTenants() {
            when(connectionManager.getConnection(TENANT_A.tenantId())).thenReturn(connection);
            when(connectionManager.getConnection(TENANT_B.tenantId())).thenReturn(connection);

            AxonServerTenantEventSegmentFactory factory = testSubject();
            EventStore storeA = factory.apply(TENANT_A);
            EventStore storeB = factory.apply(TENANT_B);

            assertThat(storeA).isNotSameAs(storeB);
        }
    }

    @Nested
    class Caching {

        @Test
        void returnsSameEventStoreForSameTenant() {
            when(connectionManager.getConnection(TENANT_A.tenantId())).thenReturn(connection);

            AxonServerTenantEventSegmentFactory factory = testSubject();
            EventStore first = factory.apply(TENANT_A);
            EventStore second = factory.apply(TENANT_A);

            assertThat(first).isSameAs(second);
        }

        @Test
        void createsConnectionOnlyOnceForSameTenant() {
            when(connectionManager.getConnection(TENANT_A.tenantId())).thenReturn(connection);

            AxonServerTenantEventSegmentFactory factory = testSubject();
            factory.apply(TENANT_A);
            factory.apply(TENANT_A);

            verify(connectionManager, times(1))
                    .getConnection(TENANT_A.tenantId());
        }
    }
}
