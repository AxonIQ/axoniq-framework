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
import io.axoniq.framework.messaging.multitenancy.util.RecordingSnapshotResolvingEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.TenantDescriptorMapping;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
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
        return new MultiTenantEventStorageEngine(new TenantEventStorage(engines::apply, snapshotStores),
                                                 new TenantRouter(tenantResolver, engines));
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

    @Test
    void describesItsTenantEventStorageAndItsTenantRouter() {
        MultiTenantEventStorageEngine testSubject = engineWith(alwaysTenant(TENANT_A));
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        testSubject.describeTo(descriptor);

        assertThat(descriptor.getDescribedProperties())
                .containsKeys("tenantEventStorage", "tenantRouter");
    }
}
