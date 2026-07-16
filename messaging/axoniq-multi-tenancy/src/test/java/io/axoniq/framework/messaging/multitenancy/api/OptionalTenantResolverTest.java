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

import io.axoniq.framework.messaging.multitenancy.api.MultiTenancyApiUtils.OptionalTenantResolver;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.axoniq.framework.messaging.multitenancy.api.MultiTenancyApiUtils.TENANT_ID_KEY;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OptionalTenantResolverTest {

    private static EventMessage eventWithTenant(@Nullable TenantDescriptor tenant) {
        return new GenericEventMessage(
                new MessageType("TestEvent"),
                "payload",
                tenant == null ? Map.of() : Map.of(TENANT_ID_KEY, tenant.tenantId())
        );
    }

    private final TenantResolver metadataResolver = new MetadataBasedTenantResolver();

    @Nested
    class ApplyingSingleMessage {

        @Test
        void returnsResolvedTenantWrappedInOptional() {
            OptionalTenantResolver resolver = new OptionalTenantResolver(
                    metadataResolver, () -> List.of(TENANT_A, TENANT_B)
            );

            Optional<TenantDescriptor> result = resolver.apply(eventWithTenant(TENANT_A));

            assertThat(result).hasValue(TENANT_A);
        }

        @Test
        void returnsEmptyOptionalWhenTenantCannotBeResolved() {
            OptionalTenantResolver resolver = new OptionalTenantResolver(
                    metadataResolver, () -> List.of(TENANT_A)
            );

            EventMessage eventWithUnknownTenant = eventWithTenant(null);

            Optional<TenantDescriptor> result = resolver.apply(eventWithUnknownTenant);

            assertThat(result).isEmpty();
        }
    }

    @Nested
    class ResolvingFromCollection {

        @Test
        void returnsResolvedTenantWhenAllMessagesHaveSameTenant() {
            OptionalTenantResolver resolver = new OptionalTenantResolver(
                    metadataResolver, TENANT_DESCRIPTORS
            );

            Optional<TenantDescriptor> result = resolver.apply(
                    List.of(eventWithTenant(TENANT_A), eventWithTenant(TENANT_A))
            );

            assertThat(result).hasValue(TENANT_A);
        }

        @Test
        void returnsEmptyOptionalForEmptyCollection() {
            OptionalTenantResolver resolver = new OptionalTenantResolver(
                    metadataResolver, () -> List.of(TENANT_A)
            );

            Optional<TenantDescriptor> result = resolver.apply(List.of());

            assertThat(result).isEmpty();
        }

        @Test
        void throwsTenantNotResolvedExceptionWhenMessagesHaveMixedTenants() {
            OptionalTenantResolver resolver = new OptionalTenantResolver(
                    metadataResolver, () -> List.of(TENANT_A, TENANT_B)
            );

            assertThatThrownBy(() -> resolver.apply(
                    List.of(eventWithTenant(TENANT_A), eventWithTenant(TENANT_B))
            ))
                    .isInstanceOf(TenantNotResolvedException.class)
                    .hasMessage(
                            "Events in a single publish batch must all belong to the same tenant, but found mixed tenants: foo-a vs foo-b");
        }
    }
}
