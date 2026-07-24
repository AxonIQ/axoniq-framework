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

package io.axoniq.framework.messaging.multitenancy.eventsourcing;

import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.util.RecordingSnapshotStore;
import io.axoniq.framework.messaging.multitenancy.util.TenantDescriptorMapping;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Map;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MultiTenantSnapshotStoreTest {

    private static final QualifiedName SNAPSHOT_NAME = new QualifiedName(String.class);
    private static final String IDENTIFIER = "identifier-1";

    private static EventMessage messageForTenant(TenantDescriptor tenant) {
        return new GenericEventMessage(new MessageType("TestEvent"),
                                       "payload",
                                       Map.of(TenantDescriptor.TENANT_ID_KEY, tenant.tenantId()));
    }

    private final TenantDescriptorMapping<SnapshotStore> stores = new TenantDescriptorMapping<>();
    private final RecordingSnapshotStore tenantA = stores.entry(TENANT_A, new RecordingSnapshotStore());
    private final RecordingSnapshotStore tenantB = stores.entry(TENANT_B, new RecordingSnapshotStore());

    @Nested
    class Loading {

        @Test
        void loadRoutesToTheTenantOnTheProcessingContext() {
            MultiTenantSnapshotStore testSubject =
                    new MultiTenantSnapshotStore(stores::apply, alwaysTenant(TENANT_B), stores);
            StubProcessingContext context = new StubProcessingContext();
            context.withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);

            testSubject.load(SNAPSHOT_NAME, IDENTIFIER, context);

            assertThat(tenantA.loadCount()).isEqualTo(1);
            assertThat(tenantB.loadCount()).isZero();
        }

        @Test
        void loadWithoutATenantResourceResolvesFromTheMessageInTheContext() {
            // no resource on the context, so the tenant is resolved from the message the context carries
            MultiTenantSnapshotStore testSubject =
                    new MultiTenantSnapshotStore(stores::apply, new MetadataBasedTenantResolver(), stores);
            ProcessingContext context = StubProcessingContext.forMessage(messageForTenant(TENANT_B));

            testSubject.load(SNAPSHOT_NAME, IDENTIFIER, context);

            assertThat(tenantB.loadCount()).isEqualTo(1);
            assertThat(tenantA.loadCount()).isZero();
        }
    }

    @Nested
    class Storing {

        @Test
        void storeRoutesToTheTenantOnTheProcessingContext() {
            MultiTenantSnapshotStore testSubject =
                    new MultiTenantSnapshotStore(stores::apply, alwaysTenant(TENANT_B), stores);
            StubProcessingContext context = new StubProcessingContext();
            context.withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);
            Snapshot snapshot = new Snapshot(new GlobalIndexPosition(0L), "0", "payload", Instant.EPOCH, Map.of());

            testSubject.store(SNAPSHOT_NAME, IDENTIFIER, snapshot, context);

            assertThat(tenantA.storeCount()).isEqualTo(1);
            assertThat(tenantB.storeCount()).isZero();
        }
    }

    @Nested
    class UnresolvedTenant {

        @Test
        void loadWithoutAContextCompletesExceptionally() {
            MultiTenantSnapshotStore testSubject =
                    new MultiTenantSnapshotStore(stores::apply, alwaysTenant(TENANT_A), stores);

            var result = testSubject.load(SNAPSHOT_NAME, IDENTIFIER, null);

            assertThat(result).isCompletedExceptionally();
            assertThatThrownBy(result::join).hasCauseInstanceOf(TenantNotResolvedException.class);
        }

        @Test
        void storeWithoutAResolvableTenantCompletesExceptionally() {
            MultiTenantSnapshotStore testSubject =
                    new MultiTenantSnapshotStore(stores::apply, new MetadataBasedTenantResolver(), stores);
            StubProcessingContext contextWithoutTenant = new StubProcessingContext();
            Snapshot snapshot = new Snapshot(new GlobalIndexPosition(0L), "0", "payload", Instant.EPOCH, Map.of());

            var result = testSubject.store(SNAPSHOT_NAME, IDENTIFIER, snapshot, contextWithoutTenant);

            assertThat(result).isCompletedExceptionally();
            assertThatThrownBy(result::join).hasCauseInstanceOf(TenantNotResolvedException.class);
        }
    }

    @Test
    void describesItsFactory() {
        MultiTenantSnapshotStore testSubject =
                new MultiTenantSnapshotStore(stores::apply, alwaysTenant(TENANT_A), stores);
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        testSubject.describeTo(descriptor);

        assertThat(descriptor.getDescribedProperties()).containsKeys("snapshotStoreFactory", "tenantResolver");
    }
}
