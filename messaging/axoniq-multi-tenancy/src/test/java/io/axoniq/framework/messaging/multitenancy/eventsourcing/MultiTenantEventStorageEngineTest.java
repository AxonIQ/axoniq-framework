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
import io.axoniq.framework.messaging.multitenancy.util.RecordingEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.TenantDescriptorMapping;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.MessageType;
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

    private final TenantDescriptorMapping<EventStorageEngine> engines = new TenantDescriptorMapping<>();
    private final RecordingEventStorageEngine tenantA = engines.entry(TENANT_A, new RecordingEventStorageEngine());
    private final RecordingEventStorageEngine tenantB = engines.entry(TENANT_B, new RecordingEventStorageEngine());

    private static EventMessage event(@Nullable TenantDescriptor tenant) {
        Map<String, String> metadata = tenant == null ? Map.of() : Map.of(TenantDescriptor.TENANT_ID_KEY, tenant.tenantId());
        return new GenericEventMessage(new MessageType("TestEvent"), "payload", metadata);
    }

    private static List<TaggedEventMessage<?>> tagged(EventMessage event) {
        return List.of(new GenericTaggedEventMessage<>(event, Set.of()));
    }

    @Nested
    class Writing {

        @Test
        void appendRoutesToTheTenantOnTheProcessingContext() {
            // resolver would say TENANT_B, but the context resource wins
            MultiTenantEventStorageEngine testSubject =
                    new MultiTenantEventStorageEngine(engines::apply, alwaysTenant(TENANT_B), engines);
            StubProcessingContext context = new StubProcessingContext();
            context.withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);

            testSubject.appendEvents(AppendCondition.none(), context, tagged(event(null)));

            assertThat(tenantA.appendCount()).isEqualTo(1);
            assertThat(tenantB.appendCount()).isZero();
        }

        @Test
        void appendWithoutContextResolvesTheTenantFromTheEventMetadata() {
            MultiTenantEventStorageEngine testSubject =
                    new MultiTenantEventStorageEngine(engines::apply, new MetadataBasedTenantResolver(), engines);

            testSubject.appendEvents(AppendCondition.none(), null, tagged(event(TENANT_B)));

            assertThat(tenantB.appendCount()).isEqualTo(1);
            assertThat(tenantA.appendCount()).isZero();
        }

        @Test
        void appendWithoutAResolvableTenantFails() {
            MultiTenantEventStorageEngine testSubject =
                    new MultiTenantEventStorageEngine(engines::apply, new MetadataBasedTenantResolver(), engines);

            var result = testSubject.appendEvents(NONE, null, tagged(event(null)));

            assertThat(result).isCompletedExceptionally();
            assertThatThrownBy(result::join).hasCauseInstanceOf(TenantNotResolvedException.class);
        }

        @Test
        void appendOfABatchSpanningTenantsFails() {
            MultiTenantEventStorageEngine testSubject =
                    new MultiTenantEventStorageEngine(engines::apply, new MetadataBasedTenantResolver(), engines);
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
            MultiTenantEventStorageEngine testSubject =
                    new MultiTenantEventStorageEngine(engines::apply, alwaysTenant(TENANT_B), engines);
            StubProcessingContext context = new StubProcessingContext();
            context.withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);

            testSubject.source(ANY, context);

            assertThat(tenantA.sourceCount()).isEqualTo(1);
            assertThat(tenantB.sourceCount()).isZero();
        }

        @Test
        void sourceWithoutAContextFails() {
            MultiTenantEventStorageEngine testSubject =
                    new MultiTenantEventStorageEngine(engines::apply, alwaysTenant(TENANT_A), engines);

            assertThat(testSubject.source(ANY, null).error())
                    .containsInstanceOf(TenantNotResolvedException.class);
        }

        @Test
        void sourceWithoutATenantResourceResolvesFromTheMessageInTheContext() {
            // no resource on the context, so the tenant is resolved from the message the context carries
            MultiTenantEventStorageEngine testSubject =
                    new MultiTenantEventStorageEngine(engines::apply, new MetadataBasedTenantResolver(), engines);
            ProcessingContext context = StubProcessingContext.forMessage(event(TENANT_B));

            testSubject.source(ANY, context);

            assertThat(tenantB.sourceCount()).isEqualTo(1);
            assertThat(tenantA.sourceCount()).isZero();
        }

        @Test
        void sourceWithAContextThatResolvesToNoTenantFails() {
            MultiTenantEventStorageEngine testSubject =
                    new MultiTenantEventStorageEngine(engines::apply, new MetadataBasedTenantResolver(), engines);
            ProcessingContext context = StubProcessingContext.forMessage(event(null));

            assertThat(testSubject.source(ANY, context).error())
                    .containsInstanceOf(TenantNotResolvedException.class);
        }
    }

    @Nested
    class Streaming {

        private final MultiTenantEventStorageEngine testSubject =
                new MultiTenantEventStorageEngine(engines::apply, alwaysTenant(TENANT_A), engines);

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
    void describesItsEngineFactory() {
        MultiTenantEventStorageEngine testSubject =
                new MultiTenantEventStorageEngine(engines::apply, alwaysTenant(TENANT_A), engines);
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        testSubject.describeTo(descriptor);

        assertThat(descriptor.getDescribedProperties()).containsKeys("engineFactory", "tenantResolver");
    }
}
