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

package io.axoniq.framework.messaging.multitenancy.api;

import io.axoniq.framework.messaging.multitenancy.util.RecordingEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.RecordingSnapshotStore;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the rule every {@link TenantEventStorageEngineFactory} applies so its per-tenant engine can resolve that
 * tenant's snapshots, in both directions: an engine that resolves snapshots itself is handed back untouched, and any
 * other engine is complemented with the tenant's snapshot store.
 *
 * @author Laura Devriendt
 */
class TenantEventStorageEngineFactoryTest {

    private static final QualifiedName SNAPSHOT_NAME = new QualifiedName(String.class);
    private static final String IDENTIFIER = "identifier-1";

    private final RecordingEventStorageEngine tenantEngine = new RecordingEventStorageEngine();
    private final RecordingSnapshotStore tenantSnapshotStore = new RecordingSnapshotStore();

    private static SourcingCondition snapshotCondition() {
        return SourcingCondition.conditionFor(new SourcingStrategy.Snapshot(SNAPSHOT_NAME, IDENTIFIER, null),
                                              EventCriteria.havingAnyTag());
    }

    @Nested
    class ApplyingTheRule {

        @Test
        void anEngineThatIsTheTenantsSnapshotStoreIsHandedBackUntouched() {
            // it resolves the snapshot within its own source call, so decorating it would cost that single round trip
            EventStorageEngine result =
                    TenantEventStorageEngineFactory.snapshotCapable(tenantEngine, tenantEngine);

            assertThat(result).isSameAs(tenantEngine);
        }

        @Test
        void anEngineThatResolvesSnapshotsButIsNotTheTenantsStoreReadsFromTheTenantsStore() {
            // the tenant's snapshots live in a different store, so reading them from the engine itself would miss them
            EventStorageEngine result =
                    TenantEventStorageEngineFactory.snapshotCapable(tenantEngine, tenantSnapshotStore);

            result.source(snapshotCondition(), null);

            assertThat(tenantSnapshotStore.loadCount()).isEqualTo(1);
            assertThat(tenantEngine.loadCount()).isZero();
        }
    }

    @Nested
    class SnapshotCapableEngineBehaviour {

        @Test
        void leadsItsStreamWithTheSnapshotFromTheTenantsStore() {
            Snapshot snapshot = new Snapshot(new GlobalIndexPosition(0L), "0", "payload", Instant.EPOCH, Map.of());
            tenantSnapshotStore.store(SNAPSHOT_NAME, IDENTIFIER, snapshot, null).join();
            EventStorageEngine complemented =
                    TenantEventStorageEngineFactory.snapshotCapable(tenantEngine, tenantSnapshotStore);

            MessageStream<EventMessage> sourced = complemented.source(snapshotCondition(), null);

            EventMessage leading = sourced.next().orElseThrow().message();
            assertThat(leading).isInstanceOf(SnapshotEventMessage.class);
            assertThat(((SnapshotEventMessage) leading).payload()).isEqualTo(snapshot);
        }

        @Test
        void passesAPlainSourcingThroughWithoutConsultingTheSnapshotStore() {
            EventStorageEngine complemented =
                    TenantEventStorageEngineFactory.snapshotCapable(tenantEngine, tenantSnapshotStore);

            complemented.source(SourcingCondition.conditionFor(EventCriteria.havingAnyTag()), null);

            assertThat(tenantEngine.sourceCount()).isEqualTo(1);
            assertThat(tenantSnapshotStore.loadCount()).isZero();
        }
    }
}
