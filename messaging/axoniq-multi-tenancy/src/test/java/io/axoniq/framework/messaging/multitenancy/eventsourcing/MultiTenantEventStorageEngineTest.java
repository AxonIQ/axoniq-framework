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
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.util.RecordingEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.RecordingSnapshotResolvingEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.RecordingSnapshotStore;
import io.axoniq.framework.messaging.multitenancy.util.TenantDescriptorMapping;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MultiTenantEventStorageEngineTest {

    private static final SourcingCondition ANY = SourcingCondition.conditionFor(EventCriteria.havingAnyTag());
    private static final AppendCondition NONE = AppendCondition.none();
    private static final QualifiedName SNAPSHOT_NAME = new QualifiedName(String.class);
    private static final String IDENTIFIER = "identifier-1";

    private final TenantDescriptorMapping<RecordingSnapshotResolvingEventStorageEngine> engines =
            new TenantDescriptorMapping<>();
    private final RecordingSnapshotResolvingEventStorageEngine tenantA =
            engines.entry(TENANT_A, new RecordingSnapshotResolvingEventStorageEngine());
    private final RecordingSnapshotResolvingEventStorageEngine tenantB =
            engines.entry(TENANT_B, new RecordingSnapshotResolvingEventStorageEngine());
    // Each tenant's engine is its own snapshot store, so it stays undecorated and receives the strategy itself.
    private final TenantSnapshotStoreFactory snapshotStores = engines::apply;

    private MultiTenantEventStorageEngine engineWith(TenantResolver tenantResolver) {
        return registered(new MultiTenantEventStorageEngine(engines::apply,
                                                           snapshotStores,
                                                           new TenantRouter(tenantResolver, engines)));
    }

    private MultiTenantEventStorageEngine engineWith(TenantEventStorageEngineFactory engineFactory,
                                                     TenantSnapshotStoreFactory snapshotStoreFactory) {
        return registered(new MultiTenantEventStorageEngine(engineFactory,
                                                           snapshotStoreFactory,
                                                           new TenantRouter(alwaysTenant(TENANT_A), engines)));
    }

    private static MultiTenantEventStorageEngine registered(MultiTenantEventStorageEngine engine) {
        engine.registerTenant(TENANT_A);
        engine.registerTenant(TENANT_B);
        return engine;
    }

    private static EventMessage event(@Nullable TenantDescriptor tenant) {
        Map<String, String> metadata = tenant == null
                ? Map.of()
                : Map.of(TenantDescriptor.TENANT_ID_KEY, tenant.tenantId());
        return new GenericEventMessage(new MessageType("TestEvent"), "payload", metadata);
    }

    private static ProcessingContext contextFor(TenantDescriptor tenant) {
        return StubProcessingContext.forMessage(event(tenant));
    }

    private static SourcingCondition snapshotCondition() {
        return SourcingCondition.conditionFor(new SourcingStrategy.Snapshot(SNAPSHOT_NAME, IDENTIFIER, null),
                                              EventCriteria.havingAnyTag());
    }

    private static Snapshot snapshot() {
        return new Snapshot(new GlobalIndexPosition(0L), "0", "payload", Instant.EPOCH, Map.of());
    }

    private static List<TaggedEventMessage<?>> tagged(EventMessage event) {
        return List.of(new GenericTaggedEventMessage<>(event, Set.of()));
    }

    @Nested
    class Writing {

        @Test
        void appendRoutesToTheTenantOnTheProcessingContext() {
            // resolver would say TENANT_B, but the context resource wins
            MultiTenantEventStorageEngine testSubject = engineWith(alwaysTenant(TENANT_B));
            StubProcessingContext context = new StubProcessingContext();
            context.withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);

            testSubject.appendEvents(AppendCondition.none(), context, tagged(event(null)));

            assertThat(tenantA.appendCount()).isEqualTo(1);
            assertThat(tenantB.appendCount()).isZero();
        }

        @Test
        void appendWithoutContextResolvesTheTenantFromTheEventMetadata() {
            MultiTenantEventStorageEngine testSubject = engineWith(new MetadataBasedTenantResolver());

            testSubject.appendEvents(AppendCondition.none(), null, tagged(event(TENANT_B)));

            assertThat(tenantB.appendCount()).isEqualTo(1);
            assertThat(tenantA.appendCount()).isZero();
        }

        @Test
        void appendWithoutAResolvableTenantFails() {
            MultiTenantEventStorageEngine testSubject = engineWith(new MetadataBasedTenantResolver());

            var result = testSubject.appendEvents(NONE, null, tagged(event(null)));

            assertThat(result).isCompletedExceptionally();
            assertThatThrownBy(result::join).hasCauseInstanceOf(TenantNotResolvedException.class);
        }

        @Test
        void appendOfABatchSpanningTenantsFails() {
            MultiTenantEventStorageEngine testSubject = engineWith(new MetadataBasedTenantResolver());
            List<TaggedEventMessage<?>> mixed = List.of(
                    new GenericTaggedEventMessage<>(event(TENANT_A), Set.of()),
                    new GenericTaggedEventMessage<>(event(TENANT_B), Set.of())
            );

            var result = testSubject.appendEvents(NONE, null, mixed);

            assertThat(result).isCompletedExceptionally();
            assertThatThrownBy(result::join).hasCauseInstanceOf(TenantNotResolvedException.class);
        }
    }

    @Nested
    class Sourcing {

        @Test
        void sourceRoutesToTheTenantOnTheProcessingContext() {
            MultiTenantEventStorageEngine testSubject = engineWith(alwaysTenant(TENANT_B));
            StubProcessingContext context = new StubProcessingContext();
            context.withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);

            testSubject.source(ANY, context);

            assertThat(tenantA.sourceCount()).isEqualTo(1);
            assertThat(tenantB.sourceCount()).isZero();
        }

        @Test
        void sourceWithoutAContextFails() {
            MultiTenantEventStorageEngine testSubject = engineWith(alwaysTenant(TENANT_A));

            assertThat(testSubject.source(ANY, null).error())
                    .containsInstanceOf(TenantNotResolvedException.class);
        }

        @Test
        void sourceWithoutATenantResourceResolvesFromTheMessageInTheContext() {
            // no resource on the context, so the tenant is resolved from the message the context carries
            MultiTenantEventStorageEngine testSubject = engineWith(new MetadataBasedTenantResolver());
            ProcessingContext context = StubProcessingContext.forMessage(event(TENANT_B));

            testSubject.source(ANY, context);

            assertThat(tenantB.sourceCount()).isEqualTo(1);
            assertThat(tenantA.sourceCount()).isZero();
        }

        @Test
        void sourceWithAContextThatResolvesToNoTenantFails() {
            MultiTenantEventStorageEngine testSubject = engineWith(new MetadataBasedTenantResolver());
            ProcessingContext context = StubProcessingContext.forMessage(event(null));

            assertThat(testSubject.source(ANY, context).error())
                    .containsInstanceOf(TenantNotResolvedException.class);
        }

        // The tenant's engine is built to resolve that tenant's snapshots, so the routing engine forwards the sourcing
        // condition unchanged. That is what lets a snapshot resolving tenant engine keep its single round trip.
        @Test
        void snapshotSourcingReachesTheTenantEngineWithTheStrategyIntact() {
            MultiTenantEventStorageEngine testSubject = engineWith(new MetadataBasedTenantResolver());

            testSubject.source(snapshotCondition(), contextFor(TENANT_A));

            assertThat(tenantA.sourcedWithSnapshotStrategy()).isTrue();
            assertThat(tenantB.sourcedWithSnapshotStrategy()).isFalse();
        }

        // A tenant whose engine resolves no snapshots is served through the decorated route, and that decoration sits
        // below the fan-out, so the snapshot comes from that tenant's own store.
        @Test
        void snapshotSourcingOfATenantWithADecoratedEngineReadsFromThatTenantsStore() {
            RecordingEventStorageEngine plainEngine = new RecordingEventStorageEngine();
            RecordingSnapshotStore storeOfTenantA = new RecordingSnapshotStore();
            RecordingSnapshotStore storeOfTenantB = new RecordingSnapshotStore();
            MultiTenantEventStorageEngine testSubject = engineWith(
                    tenant -> plainEngine,
                    tenant -> TENANT_A.equals(tenant) ? storeOfTenantA : storeOfTenantB);

            testSubject.source(snapshotCondition(), contextFor(TENANT_A));

            assertThat(storeOfTenantA.loadCount()).isEqualTo(1);
            assertThat(storeOfTenantB.loadCount()).isZero();
            // the strategy was resolved by the decoration, so the tenant's engine was sourced by position
            assertThat(plainEngine.sourcedWithSnapshotStrategy()).isFalse();
            assertThat(plainEngine.sourceCount()).isEqualTo(1);
        }

        // Routing never resolves a snapshot itself, so a plain sourcing must not touch any tenant's snapshot store.
        @Test
        void nonSnapshotSourcingDoesNotConsultAnyTenantSnapshotStore() {
            MultiTenantEventStorageEngine testSubject = engineWith(new MetadataBasedTenantResolver());

            testSubject.source(ANY, contextFor(TENANT_A));

            assertThat(tenantA.loadCount()).isZero();
            assertThat(tenantB.loadCount()).isZero();
        }
    }

    @Nested
    class Streaming {

        private final MultiTenantEventStorageEngine testSubject = engineWith(alwaysTenant(TENANT_A));

        @Test
        void streamingSpansAllTenantsAndIsNotSupportedByTheRoutingEngine() {
            StreamingCondition fromStart = StreamingCondition.startingFrom(null);

            assertThatThrownBy(() -> testSubject.stream(fromStart)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(testSubject::firstToken).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(testSubject::latestToken).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> testSubject.tokenAt(Instant.EPOCH))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    /**
     * Verifies that a tenant's engine is composed to resolve that tenant's snapshots, along both routes: an engine
     * resolving snapshots itself keeps its single round trip, and any other engine is complemented with that tenant's
     * snapshot store.
     */
    @Nested
    class AnEngineResolvingSnapshotsItself {

        private final RecordingSnapshotResolvingEventStorageEngine tenantEngine =
                new RecordingSnapshotResolvingEventStorageEngine();

        // Its snapshot store for this tenant is the engine itself, as a PostgresqlEventStorageEngine tenant factory
        // would return.
        private MultiTenantEventStorageEngine testSubject() {
            return engineWith(tenant -> tenantEngine, tenant -> tenantEngine);
        }

        @Test
        void isComposedIntoTheEngineItself() {
            // decorating it would resolve the snapshot separately, costing it that single round trip
            assertThat(testSubject().engineFor(TENANT_A)).isSameAs(tenantEngine);
        }

        @Test
        void receivesTheSnapshotSourcingStrategyItself() {
            testSubject().engineFor(TENANT_A).source(snapshotCondition(), null);

            assertThat(tenantEngine.sourcedWithSnapshotStrategy()).isTrue();
            // read from its own storage, within that one call
            assertThat(tenantEngine.loadCount()).isEqualTo(1);
        }

        // The single round trip this path exists for: the engine serves the snapshot and the events following it from
        // its own storage, within the one source call.
        @Test
        void leadsItsStreamWithItsOwnSnapshotInASingleCall() {
            Snapshot snapshot = snapshot();
            tenantEngine.store(SNAPSHOT_NAME, IDENTIFIER, snapshot, null).join();

            MessageStream<EventMessage> sourced = testSubject().engineFor(TENANT_A)
                                                               .source(snapshotCondition(), null);

            EventMessage leading = sourced.next().orElseThrow().message();
            assertThat(leading).isInstanceOf(SnapshotEventMessage.class);
            assertThat(((SnapshotEventMessage) leading).payload()).isEqualTo(snapshot);
            assertThat(tenantEngine.sourceCount()).isEqualTo(1);
        }
    }

    @Nested
    class AnEngineResolvingNoSnapshots {

        private final RecordingEventStorageEngine tenantEngine = new RecordingEventStorageEngine();
        private final RecordingSnapshotStore tenantSnapshotStore = new RecordingSnapshotStore();

        private MultiTenantEventStorageEngine testSubject() {
            return engineWith(tenant -> tenantEngine, tenant -> tenantSnapshotStore);
        }

        @Test
        void readsTheSnapshotFromTheTenantsStore() {
            testSubject().engineFor(TENANT_A).source(snapshotCondition(), null);

            assertThat(tenantSnapshotStore.loadCount()).isEqualTo(1);
            // the strategy is resolved above the engine, which is then sourced by position
            assertThat(tenantEngine.sourcedWithSnapshotStrategy()).isFalse();
        }

        @Test
        void leadsItsStreamWithTheSnapshotFromTheTenantsStore() {
            Snapshot snapshot = snapshot();
            tenantSnapshotStore.store(SNAPSHOT_NAME, IDENTIFIER, snapshot, null).join();

            MessageStream<EventMessage> sourced = testSubject().engineFor(TENANT_A)
                                                               .source(snapshotCondition(), null);

            EventMessage leading = sourced.next().orElseThrow().message();
            assertThat(leading).isInstanceOf(SnapshotEventMessage.class);
            assertThat(((SnapshotEventMessage) leading).payload()).isEqualTo(snapshot);
        }

        @Test
        void passesAPlainSourcingThroughWithoutConsultingTheSnapshotStore() {
            testSubject().engineFor(TENANT_A).source(ANY, null);

            assertThat(tenantEngine.sourceCount()).isEqualTo(1);
            assertThat(tenantSnapshotStore.loadCount()).isZero();
        }
    }

    @Nested
    class Composition {

        private final RecordingSnapshotStore tenantSnapshotStore = new RecordingSnapshotStore();

        // The decision is taken per tenant, so a tenant served by a snapshot resolving engine and one served by a plain
        // engine must each get their own treatment from the same routing engine.
        @Test
        void decidesPerTenantWhenTenantsDifferInHowTheyResolveSnapshots() {
            RecordingSnapshotResolvingEventStorageEngine selfResolving =
                    new RecordingSnapshotResolvingEventStorageEngine();
            RecordingEventStorageEngine plain = new RecordingEventStorageEngine();
            MultiTenantEventStorageEngine testSubject = engineWith(
                    tenant -> TENANT_A.equals(tenant) ? selfResolving : plain,
                    tenant -> TENANT_A.equals(tenant) ? selfResolving : tenantSnapshotStore);

            testSubject.engineFor(TENANT_A).source(snapshotCondition(), null);
            testSubject.engineFor(TENANT_B).source(snapshotCondition(), null);

            // tenant A served the strategy itself, tenant B had it resolved from that tenant's own store
            assertThat(selfResolving.sourcedWithSnapshotStrategy()).isTrue();
            assertThat(plain.sourcedWithSnapshotStrategy()).isFalse();
            assertThat(tenantSnapshotStore.loadCount()).isEqualTo(1);
        }

        // A tenant's engine and snapshot store are gone once the tenant is removed, so a composed engine holding them
        // must not survive it, and composing is not repeated per operation while the tenant is there.
        @Test
        void composesOncePerTenantAndAgainAfterTheTenantIsReAdded() {
            RecordingEventStorageEngine tenantEngine = new RecordingEventStorageEngine();
            MultiTenantEventStorageEngine testSubject =
                    new MultiTenantEventStorageEngine(tenant -> tenantEngine,
                                                      tenant -> tenantSnapshotStore,
                                                      new TenantRouter(alwaysTenant(TENANT_A), engines));
            Registration registration = testSubject.registerTenant(TENANT_A);

            EventStorageEngine composed = testSubject.engineFor(TENANT_A);
            assertThat(testSubject.engineFor(TENANT_A)).isSameAs(composed);

            assertThat(registration.cancel()).isTrue();

            assertThatThrownBy(() -> testSubject.engineFor(TENANT_A))
                    .isInstanceOf(TenantNotResolvedException.class);

            // A re-added tenant never continues with the engine composed under its previous registration. That the
            // previous engine is also let go of is not observable here, since this engine only drops its reference to
            // it. The eviction itself is asserted where the callback exists, in TenantScopedCacheTest.
            testSubject.registerTenant(TENANT_A);
            assertThat(testSubject.engineFor(TENANT_A)).isNotSameAs(composed);
        }
    }


    @Nested
    class Construction {

        private final TenantRouter tenantRouter = new TenantRouter(alwaysTenant(TENANT_A), engines);

        @Test
        void rejectsANullEngineFactory() {
            assertThatThrownBy(() -> new MultiTenantEventStorageEngine(null, snapshotStores, tenantRouter))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("The tenant event storage engine factory must not be null");
        }

        @Test
        void rejectsANullSnapshotStoreFactory() {
            TenantEventStorageEngineFactory engineFactory = tenant -> new RecordingEventStorageEngine();

            assertThatThrownBy(() -> new MultiTenantEventStorageEngine(engineFactory, null, tenantRouter))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("The tenant snapshot store factory must not be null");
        }
    }

    @Test
    void describesItsFactoriesAndItsTenantRouter() {
        MultiTenantEventStorageEngine testSubject = engineWith(alwaysTenant(TENANT_A));
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        testSubject.describeTo(descriptor);

        assertThat(descriptor.getDescribedProperties())
                .containsKeys("engineFactory", "snapshotStoreFactory", "tenantRouter");
    }
}
