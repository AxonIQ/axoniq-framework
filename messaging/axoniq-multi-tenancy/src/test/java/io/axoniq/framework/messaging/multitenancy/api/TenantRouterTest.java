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

import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor.TENANT_ID_KEY;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TenantRouterTest {

    private static EventMessage eventWithTenant(@Nullable TenantDescriptor tenant) {
        return new GenericEventMessage(
                new MessageType("TestEvent"),
                "payload",
                tenant == null ? Map.of() : Map.of(TENANT_ID_KEY, tenant.tenantId())
        );
    }

    private final TenantResolver metadataResolver = new MetadataBasedTenantResolver();

    @Nested
    class ResolvingFromContext {

        @Test
        void resolvesTheKnownTenantFromTheContextResource() {
            TenantRouter resolver = new TenantRouter(metadataResolver, TENANT_DESCRIPTORS);
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);

            assertThat(resolver.resolveFromContext(context)).hasValue(TENANT_A);
        }

        // A tenant resource that is present decides on its own. Falling back to the message would let its metadata
        // redirect the operation to another tenant's store whenever the resource names an unknown tenant.
        @Test
        void failsWhenTheContextResourceTenantIsNotKnownEvenIfTheMessageNamesAKnownTenant() {
            TenantRouter resolver = new TenantRouter(metadataResolver, () -> List.of(TENANT_A));
            ProcessingContext context = StubProcessingContext.forMessage(eventWithTenant(TENANT_A))
                    .withResource(TenantDescriptor.RESOURCE_KEY, TENANT_B);

            assertThatThrownBy(() -> resolver.resolveFromContext(context))
                    .isInstanceOf(TenantNotResolvedException.class)
                    .hasMessageContaining(TENANT_B.tenantId());
        }

        @Test
        void returnsEmptyWhenNeitherTheResourceNorTheMessageNamesAKnownTenant() {
            TenantRouter resolver = new TenantRouter(metadataResolver, () -> List.of(TENANT_A));
            ProcessingContext context = StubProcessingContext.forMessage(eventWithTenant(null));

            assertThat(resolver.resolveFromContext(context)).isEmpty();
        }

        @Test
        void returnsEmptyForANullContext() {
            TenantRouter resolver = new TenantRouter(metadataResolver, TENANT_DESCRIPTORS);

            assertThat(resolver.resolveFromContext(null)).isEmpty();
        }
    }

    @Nested
    class ResolvingFromMessage {

        @Test
        void resolvesTheKnownTenantNamedByTheMessage() {
            TenantRouter resolver = new TenantRouter(metadataResolver, TENANT_DESCRIPTORS);

            assertThat(resolver.resolveFromMessage(eventWithTenant(TENANT_B))).hasValue(TENANT_B);
        }

        @Test
        void returnsEmptyWhenTheMessageNamesATenantThatIsNotKnown() {
            TenantRouter resolver = new TenantRouter(metadataResolver, () -> List.of(TENANT_A));

            assertThat(resolver.resolveFromMessage(eventWithTenant(TENANT_B))).isEmpty();
        }

        @Test
        void returnsEmptyWhenTheMessageNamesNoTenant() {
            TenantRouter resolver = new TenantRouter(metadataResolver, TENANT_DESCRIPTORS);

            assertThat(resolver.resolveFromMessage(eventWithTenant(null))).isEmpty();
        }
    }

    @Nested
    class ResolvingFromCollection {

        @Test
        void returnsResolvedTenantWhenAllMessagesHaveSameTenant() {
            TenantRouter resolver = new TenantRouter(metadataResolver, TENANT_DESCRIPTORS);

            Optional<TenantDescriptor> result = resolver.resolveSharedTenant(
                    List.of(eventWithTenant(TENANT_A), eventWithTenant(TENANT_A))
            );

            assertThat(result).hasValue(TENANT_A);
        }

        @Test
        void returnsEmptyOptionalForEmptyCollection() {
            TenantRouter resolver = new TenantRouter(metadataResolver, () -> List.of(TENANT_A));

            Optional<TenantDescriptor> result = resolver.resolveSharedTenant(List.of());

            assertThat(result).isEmpty();
        }

        @Test
        void returnsEmptyWhenAnyMessageCannotBeResolvedRatherThanDroppingIt() {
            // a batch mixing a resolvable and an unresolvable event must not silently route to the resolvable tenant
            TenantRouter resolver = new TenantRouter(metadataResolver, () -> List.of(TENANT_A));

            Optional<TenantDescriptor> result = resolver.resolveSharedTenant(
                    List.of(eventWithTenant(TENANT_A), eventWithTenant(null))
            );

            assertThat(result).isEmpty();
        }

        @Test
        void returnsEmptyWhenAMessageResolvesToAnUnknownTenant() {
            // the message names foo-b, but only foo-a is a known tenant, so it must not route
            TenantRouter resolver = new TenantRouter(metadataResolver, () -> List.of(TENANT_A));

            assertThat(resolver.resolveSharedTenant(List.of(eventWithTenant(TENANT_B)))).isEmpty();
        }

        @Test
        void throwsTenantNotResolvedExceptionWhenMessagesHaveMixedTenants() {
            TenantRouter resolver = new TenantRouter(metadataResolver, TENANT_DESCRIPTORS);
            List<EventMessage> mixed = List.of(eventWithTenant(TENANT_A), eventWithTenant(TENANT_B));
            String expectedMessage =
                    "The given messages must all belong to the same tenant, but resolved to: [foo-a, foo-b]";

            assertThatThrownBy(() -> resolver.resolveSharedTenant(mixed))
                    .isInstanceOf(TenantNotResolvedException.class)
                    .hasMessage(expectedMessage);
        }
    }

    @Test
    void describesItsWrappedResolver() {
        TenantRouter resolver = new TenantRouter(metadataResolver, TENANT_DESCRIPTORS);
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        resolver.describeTo(descriptor);

        assertThat(descriptor.getDescribedProperties()).containsKey("tenantResolver");
    }
}
