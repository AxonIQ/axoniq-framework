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

class RoutingTenantResolverTest {

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
            RoutingTenantResolver resolver = new RoutingTenantResolver(metadataResolver, TENANT_DESCRIPTORS);
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);

            assertThat(resolver.resolveFromContext(context)).hasValue(TENANT_A);
        }

        @Test
        void fallsBackToTheMessageWhenTheContextResourceTenantIsNotKnown() {
            // the resource names foo-b, which is not a known tenant, so the known foo-a from the message wins
            RoutingTenantResolver resolver = new RoutingTenantResolver(metadataResolver, () -> List.of(TENANT_A));
            ProcessingContext context = StubProcessingContext.forMessage(eventWithTenant(TENANT_A))
                    .withResource(TenantDescriptor.RESOURCE_KEY, TENANT_B);

            assertThat(resolver.resolveFromContext(context)).hasValue(TENANT_A);
        }

        @Test
        void returnsEmptyForANullContext() {
            RoutingTenantResolver resolver = new RoutingTenantResolver(metadataResolver, TENANT_DESCRIPTORS);

            assertThat(resolver.resolveFromContext(null)).isEmpty();
        }
    }

    @Nested
    class ResolvingFromCollection {

        @Test
        void returnsResolvedTenantWhenAllMessagesHaveSameTenant() {
            RoutingTenantResolver resolver = new RoutingTenantResolver(metadataResolver, TENANT_DESCRIPTORS);

            Optional<TenantDescriptor> result = resolver.resolveSharedTenant(
                    List.of(eventWithTenant(TENANT_A), eventWithTenant(TENANT_A))
            );

            assertThat(result).hasValue(TENANT_A);
        }

        @Test
        void returnsEmptyOptionalForEmptyCollection() {
            RoutingTenantResolver resolver = new RoutingTenantResolver(metadataResolver, () -> List.of(TENANT_A));

            Optional<TenantDescriptor> result = resolver.resolveSharedTenant(List.of());

            assertThat(result).isEmpty();
        }

        @Test
        void returnsEmptyWhenAnyMessageCannotBeResolvedRatherThanDroppingIt() {
            // a batch mixing a resolvable and an unresolvable event must not silently route to the resolvable tenant
            RoutingTenantResolver resolver = new RoutingTenantResolver(metadataResolver, () -> List.of(TENANT_A));

            Optional<TenantDescriptor> result = resolver.resolveSharedTenant(
                    List.of(eventWithTenant(TENANT_A), eventWithTenant(null))
            );

            assertThat(result).isEmpty();
        }

        @Test
        void returnsEmptyWhenAMessageResolvesToAnUnknownTenant() {
            // the message names foo-b, but only foo-a is a known tenant, so it must not route
            RoutingTenantResolver resolver = new RoutingTenantResolver(metadataResolver, () -> List.of(TENANT_A));

            assertThat(resolver.resolveSharedTenant(List.of(eventWithTenant(TENANT_B)))).isEmpty();
        }

        @Test
        void throwsTenantNotResolvedExceptionWhenMessagesHaveMixedTenants() {
            RoutingTenantResolver resolver = new RoutingTenantResolver(metadataResolver, TENANT_DESCRIPTORS);
            List<EventMessage> mixed = List.of(eventWithTenant(TENANT_A), eventWithTenant(TENANT_B));
            String expectedMessage =
                    "Events in a single publish batch must all belong to the same tenant, but found: [foo-a, foo-b]";

            assertThatThrownBy(() -> resolver.resolveSharedTenant(mixed))
                    .isInstanceOf(TenantNotResolvedException.class)
                    .hasMessage(expectedMessage);
        }
    }

    @Test
    void describesItsWrappedResolver() {
        RoutingTenantResolver resolver = new RoutingTenantResolver(metadataResolver, TENANT_DESCRIPTORS);
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        resolver.describeTo(descriptor);

        assertThat(descriptor.getDescribedProperties()).containsKey("tenantResolver");
    }
}
