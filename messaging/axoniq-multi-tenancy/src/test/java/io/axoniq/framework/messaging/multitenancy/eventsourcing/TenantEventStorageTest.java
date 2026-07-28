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

import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.util.RecordingEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.RecordingSnapshotResolvingEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.RecordingSnapshotStore;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Map;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that {@link TenantEventStorage} hands out a tenant engine able to resolve that tenant's snapshots,
 * along both routes: an engine resolving snapshots itself keeps its single round trip, and any other engine is
 * complemented with that tenant's snapshot store.
 */
class TenantEventStorageTest {

    private static final QualifiedName SNAPSHOT_NAME = new QualifiedName(String.class);
    private static final String IDENTIFIER = "identifier-1";

    private final RecordingSnapshotStore tenantSnapshotStore = new RecordingSnapshotStore();

    private TenantEventStorage storageFor(EventStorageEngine tenantEngine) {
        return registered(new TenantEventStorage(tenant -> tenantEngine, tenant -> tenantSnapshotStore));
    }

    private static TenantEventStorage registered(TenantEventStorage storage) {
        storage.registerTenant(TENANT_A);
        storage.registerTenant(TENANT_B);
        return storage;
    }

    private static SourcingCondition snapshotCondition() {
        return SourcingCondition.conditionFor(new SourcingStrategy.Snapshot(SNAPSHOT_NAME, IDENTIFIER, null),
                                              EventCriteria.havingAnyTag());
    }

    @Nested
    class AnEngineResolvingSnapshotsItself {

        private final RecordingSnapshotResolvingEventStorageEngine tenantEngine =
                new RecordingSnapshotResolvingEventStorageEngine();

        // Its snapshot store for this tenant is the engine itself, as a PostgresqlEventStorageEngine tenant factory
        // would return.
        private TenantEventStorage storage() {
            return registered(new TenantEventStorage(tenant -> tenantEngine, tenant -> tenantEngine));
        }

        @Test
        void isHandedBackUntouched() {
            // decorating it would resolve the snapshot separately, costing it that single round trip
            assertThat(storage().engineFor(TENANT_A)).isSameAs(tenantEngine);
        }

        @Test
        void receivesTheSnapshotSourcingStrategyItself() {
            storage().engineFor(TENANT_A).source(snapshotCondition(), null);

            assertThat(tenantEngine.sourcedWithSnapshotStrategy()).isTrue();
            // read from its own storage, within that one call
            assertThat(tenantEngine.loadCount()).isEqualTo(1);
        }

        // The single round trip this path exists for: the engine serves the snapshot and the events following it from
        // its own storage, within the one source call.
        @Test
        void leadsItsStreamWithItsOwnSnapshotInASingleCall() {
            Snapshot snapshot = new Snapshot(new GlobalIndexPosition(0L), "0", "payload", Instant.EPOCH, Map.of());
            tenantEngine.store(SNAPSHOT_NAME, IDENTIFIER, snapshot, null).join();

            MessageStream<EventMessage> sourced = storage().engineFor(TENANT_A).source(snapshotCondition(), null);

            EventMessage leading = sourced.next().orElseThrow().message();
            assertThat(leading).isInstanceOf(SnapshotEventMessage.class);
            assertThat(((SnapshotEventMessage) leading).payload()).isEqualTo(snapshot);
            assertThat(tenantEngine.sourceCount()).isEqualTo(1);
        }
    }

    @Nested
    class AnEngineResolvingNoSnapshots {

        private final RecordingEventStorageEngine tenantEngine = new RecordingEventStorageEngine();

        @Test
        void readsTheSnapshotFromTheTenantsStore() {
            storageFor(tenantEngine).engineFor(TENANT_A).source(snapshotCondition(), null);

            assertThat(tenantSnapshotStore.loadCount()).isEqualTo(1);
            // the strategy is resolved above the engine, which is then sourced by position
            assertThat(tenantEngine.sourcedWithSnapshotStrategy()).isFalse();
        }

        @Test
        void leadsItsStreamWithTheSnapshotFromTheTenantsStore() {
            Snapshot snapshot = new Snapshot(new GlobalIndexPosition(0L), "0", "payload", Instant.EPOCH, Map.of());
            tenantSnapshotStore.store(SNAPSHOT_NAME, IDENTIFIER, snapshot, null).join();

            MessageStream<EventMessage> sourced =
                    storageFor(tenantEngine).engineFor(TENANT_A).source(snapshotCondition(), null);

            EventMessage leading = sourced.next().orElseThrow().message();
            assertThat(leading).isInstanceOf(SnapshotEventMessage.class);
            assertThat(((SnapshotEventMessage) leading).payload()).isEqualTo(snapshot);
        }

        @Test
        void passesAPlainSourcingThroughWithoutConsultingTheSnapshotStore() {
            storageFor(tenantEngine).engineFor(TENANT_A)
                                    .source(SourcingCondition.conditionFor(EventCriteria.havingAnyTag()), null);

            assertThat(tenantEngine.sourceCount()).isEqualTo(1);
            assertThat(tenantSnapshotStore.loadCount()).isZero();
        }
    }

    // The decision is taken per tenant, so a tenant served by a snapshot resolving engine and one served by a plain
    // engine must each get their own treatment from the same storage.
    @Test
    void decidesPerTenantWhenTenantsDifferInHowTheyResolveSnapshots() {
        RecordingSnapshotResolvingEventStorageEngine selfResolving = new RecordingSnapshotResolvingEventStorageEngine();
        RecordingEventStorageEngine plain = new RecordingEventStorageEngine();
        TenantEventStorage testSubject = registered(new TenantEventStorage(
                tenant -> TENANT_A.equals(tenant) ? selfResolving : plain,
                tenant -> TENANT_A.equals(tenant) ? selfResolving : tenantSnapshotStore
        ));

        testSubject.engineFor(TENANT_A).source(snapshotCondition(), null);
        testSubject.engineFor(TENANT_B).source(snapshotCondition(), null);

        // tenant A served the strategy itself, tenant B had it resolved from that tenant's own store
        assertThat(selfResolving.sourcedWithSnapshotStrategy()).isTrue();
        assertThat(plain.sourcedWithSnapshotStrategy()).isFalse();
        assertThat(tenantSnapshotStore.loadCount()).isEqualTo(1);
    }

    // A tenant's engine and snapshot store are gone once the tenant is removed, so a composed engine holding them must
    // not survive it, and composing is not repeated per operation while the tenant is there.
    @Test
    void composesOncePerTenantAndAgainAfterTheTenantIsReAdded() {
        RecordingEventStorageEngine tenantEngine = new RecordingEventStorageEngine();
        TenantEventStorage testSubject =
                new TenantEventStorage(tenant -> tenantEngine, tenant -> tenantSnapshotStore);
        Registration registration = testSubject.registerTenant(TENANT_A);

        EventStorageEngine composed = testSubject.engineFor(TENANT_A);
        assertThat(testSubject.engineFor(TENANT_A)).isSameAs(composed);

        assertThat(registration.cancel()).isTrue();

        assertThatThrownBy(() -> testSubject.engineFor(TENANT_A))
                .isInstanceOf(TenantNotResolvedException.class);

        // A re-added tenant never continues with the engine composed under its previous registration. That the previous
        // engine is also let go of is not observable here, since this composer only drops its reference to it. The
        // eviction itself is asserted where the callback exists, in TenantScopedCacheTest.
        testSubject.registerTenant(TENANT_A);
        assertThat(testSubject.engineFor(TENANT_A)).isNotSameAs(composed);
    }

    @Test
    void describesItsFactories() {
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        storageFor(new RecordingEventStorageEngine()).describeTo(descriptor);

        assertThat(descriptor.getDescribedProperties()).containsKeys("engineFactory", "snapshotStoreFactory");
    }

    @Nested
    class Construction {

        @Test
        void rejectsANullEngineFactory() {
            assertThatThrownBy(() -> new TenantEventStorage(null, tenant -> tenantSnapshotStore))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("The tenant event storage engine factory must not be null");
        }

        @Test
        void rejectsANullSnapshotStoreFactory() {
            TenantEventStorageEngineFactory engineFactory = tenant -> new RecordingEventStorageEngine();

            assertThatThrownBy(() -> new TenantEventStorage(engineFactory, null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("The tenant snapshot store factory must not be null");
        }
    }
}
