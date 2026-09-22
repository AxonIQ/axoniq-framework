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

import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.sequencing.SequencingPolicy;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor.TENANT_ID_KEY;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_DESCRIPTORS;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.alwaysTenant;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TenantSequencingPolicyTest {

    private final TenantSequencingPolicy testSubject = tenantSequencingPolicy(
            new MetadataBasedTenantResolver(),
            TENANT_DESCRIPTORS
    );

    @Nested
    class SequenceIdentification {

        @Test
        void returnsTheResolvedTenantIdentifierAsSequenceIdentifier() {
            // given
            Message message = commandWithTenant(TENANT_A);

            // when
            var result = testSubject.sequenceIdentifierFor(message, StubProcessingContext.forMessage(message));

            // then
            assertThat(result).hasValue(TENANT_A.tenantId());
        }

        @Test
        void returnsEqualSequenceIdentifiersForMessagesResolvingToTheSameTenant() {
            // given
            Message first = commandWithTenant(TENANT_A);
            Message second = eventWithTenant(TENANT_A);

            // when
            Object firstIdentifier = sequenceIdentifierFor(first);
            Object secondIdentifier = sequenceIdentifierFor(second);

            // then
            assertThat(firstIdentifier).isEqualTo(secondIdentifier);
        }

        @Test
        void returnsDifferentSequenceIdentifiersForMessagesResolvingToDifferentTenants() {
            // given
            Message first = commandWithTenant(TENANT_A);
            Message second = eventWithTenant(TENANT_B);

            // when
            Object firstIdentifier = sequenceIdentifierFor(first);
            Object secondIdentifier = sequenceIdentifierFor(second);

            // then
            assertThat(firstIdentifier).isNotEqualTo(secondIdentifier);
        }

        @Test
        void resolvesTheTenantFromTheContextBeforeTheMessage() {
            // given
            Message message = commandWithTenant(TENANT_A);
            var context = StubProcessingContext.forMessage(message)
                                               .withResource(TenantDescriptor.RESOURCE_KEY, TENANT_B);

            // when
            var result = testSubject.sequenceIdentifierFor(message, context);

            // then
            assertThat(result).hasValue(TENANT_B.tenantId());
        }

        @Test
        void resolvesTheTenantFromTheMessageWhenTheContextHasNoTenantResource() {
            // given
            Message message = commandWithTenant(TENANT_A);
            var context = StubProcessingContext.forMessage(message);

            // when
            var result = testSubject.sequenceIdentifierFor(message, context);

            // then
            assertThat(result).hasValue(TENANT_A.tenantId());
        }

        @Test
        void resolvesTheTenantFromTheMessageWhenNoContextIsAvailable() {
            // given
            Message message = commandWithTenant(TENANT_A);

            // when
            var result = testSubject.sequenceIdentifierFor(message, null);

            // then
            assertThat(result).hasValue(TENANT_A.tenantId());
        }

        @Test
        void returnsEmptyWhenNoKnownTenantCanBeResolved() {
            // given
            EventMessage message = EventTestUtils.asEventMessage("payload");

            // when
            var result = testSubject.sequenceIdentifierFor(message, StubProcessingContext.forMessage(message));

            // then
            assertThat(result).isEmpty();
        }

        @Test
        void returnsEmptyWhenTheMessageNamesAnUnknownTenant() {
            // given
            TenantSequencingPolicy policy = tenantSequencingPolicy(
                    new MetadataBasedTenantResolver(),
                    () -> List.of(TENANT_A)
            );
            EventMessage message = eventWithTenant(TENANT_B);

            // when
            var result = policy.sequenceIdentifierFor(message, StubProcessingContext.forMessage(message));

            // then
            assertThat(result).isEmpty();
        }

        @Test
        void failsWhenTheContextCarriesAnUnknownTenant() {
            // given
            TenantSequencingPolicy policy = tenantSequencingPolicy(
                    new MetadataBasedTenantResolver(),
                    () -> List.of(TENANT_A)
            );
            Message message = commandWithTenant(TENANT_A);
            var context = StubProcessingContext.forMessage(message)
                                               .withResource(TenantDescriptor.RESOURCE_KEY, TENANT_B);

            // when / then
            assertThatThrownBy(() -> policy.sequenceIdentifierFor(message, context))
                    .isInstanceOf(TenantNotResolvedException.class)
                    .hasMessageContaining(TENANT_B.tenantId());
        }

        @Test
        void usesTheConfiguredTenantResolver() {
            // given
            TenantSequencingPolicy policy = tenantSequencingPolicy(alwaysTenant(TENANT_B), TENANT_DESCRIPTORS);
            Message message = commandWithTenant(TENANT_A);

            // when
            var result = policy.sequenceIdentifierFor(message, StubProcessingContext.forMessage(message));

            // then
            assertThat(result).hasValue(TENANT_B.tenantId());
        }
    }

    @Nested
    class ContractCompatibility {

        @Test
        void canSequenceCommandMessages() {
            // given
            SequencingPolicy<? super CommandMessage> policy = testSubject;
            CommandMessage command = commandWithTenant(TENANT_A);

            // when
            var result = policy.sequenceIdentifierFor(command, StubProcessingContext.forMessage(command));

            // then
            assertThat(result).hasValue(TENANT_A.tenantId());
        }

        @Test
        void canSequenceEventMessages() {
            // given
            SequencingPolicy<? super EventMessage> policy = testSubject;
            EventMessage event = eventWithTenant(TENANT_A);

            // when
            var result = policy.sequenceIdentifierFor(event, StubProcessingContext.forMessage(event));

            // then
            assertThat(result).hasValue(TENANT_A.tenantId());
        }

        @Test
        void canBeCreatedFromTenantRouter() {
            // given
            TenantSequencingPolicy policy = TenantSequencingPolicy.from(
                    new TenantRouter(new MetadataBasedTenantResolver(), TENANT_DESCRIPTORS)
            );
            Message message = commandWithTenant(TENANT_A);

            // when
            var result = policy.sequenceIdentifierFor(message, StubProcessingContext.forMessage(message));

            // then
            assertThat(result).hasValue(TENANT_A.tenantId());
        }
    }

    private Object sequenceIdentifierFor(Message message) {
        return testSubject.sequenceIdentifierFor(message, StubProcessingContext.forMessage(message)).orElseThrow();
    }

    private static TenantSequencingPolicy tenantSequencingPolicy(TenantResolver tenantResolver,
                                                                TenantDescriptors tenantDescriptors) {
        return TenantSequencingPolicy.from(new TenantRouter(tenantResolver, tenantDescriptors));
    }

    private static CommandMessage commandWithTenant(TenantDescriptor tenant) {
        return new GenericCommandMessage(
                new MessageType("TestCommand"),
                "payload",
                Map.of(TENANT_ID_KEY, tenant.tenantId())
        );
    }

    private static EventMessage eventWithTenant(TenantDescriptor tenant) {
        return EventTestUtils.asEventMessage("payload")
                             .andMetadata(Map.of(TENANT_ID_KEY, tenant.tenantId()));
    }
}
