/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.api;

import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataBasedTenantResolverTest {

    private final MetadataBasedTenantResolver testSubject = new MetadataBasedTenantResolver();

    @Test
    void resolvesRegisteredTenantDescriptorWhenPropertiesDiffer() {
        Message message = new GenericMessage(
                "message-id",
                new MessageType("TestCommand"),
                "payload".getBytes(),
                Map.of(DEFAULT_TENANT_METADATA_KEY, TENANT_A.tenantId())
        );

        TenantDescriptor resolved = testSubject.resolveTenant(message, List.of(TENANT_A));

        assertThat(resolved).isSameAs(TENANT_A);
    }

    @Test
    void fallsBackToTenantIdOnlyDescriptorWhenTenantIsUnknown() {
        Message message = new GenericMessage(
                "message-id",
                new MessageType("TestCommand"),
                "payload".getBytes(),
                Map.of(DEFAULT_TENANT_METADATA_KEY, TENANT_B.tenantId())
        );

        TenantDescriptor resolved = testSubject.resolveTenant(message, List.of(TENANT_A));

        assertThat(resolved).isEqualTo(TenantDescriptor.tenantWithId(TENANT_B.tenantId()));
    }

    @Test
    void failsWhenTenantKeyIsMissing() {
        Message message = new GenericMessage(
                "message-id",
                new MessageType("TestCommand"),
                "payload".getBytes(),
                Collections.emptyMap()
        );

        assertThatThrownBy(() -> testSubject.resolveTenant(message, List.of(TENANT_A)))
                .isInstanceOf(TenantNotResolvedException.class)
                .hasMessageContaining("No tenant identifier found in message metadata under key '" + DEFAULT_TENANT_METADATA_KEY + "'");
    }

    @Nested
    class AttachTenant {

        @Test
        void attachesTheTenantIdentifierUnderTheConfiguredMetadataKey() {
            Message message = new GenericMessage(
                    "message-id",
                    new MessageType("TestCommand"),
                    "payload".getBytes(),
                    Collections.emptyMap()
            );

            Message attached = testSubject.attachTenant(message, TENANT_A);

            assertThat(attached.metadata().get(DEFAULT_TENANT_METADATA_KEY)).isEqualTo(TENANT_A.tenantId());
        }

        @Test
        void attachesUnderACustomMetadataKeyWhenConfiguredWithOne() {
            MetadataBasedTenantResolver customKeyResolver = new MetadataBasedTenantResolver("customTenantKey");
            Message message = new GenericMessage(
                    "message-id",
                    new MessageType("TestCommand"),
                    "payload".getBytes(),
                    Collections.emptyMap()
            );

            Message attached = customKeyResolver.attachTenant(message, TENANT_A);

            assertThat(attached.metadata().get("customTenantKey")).isEqualTo(TENANT_A.tenantId());
        }

        @Test
        void preservesExistingMetadataEntriesWhenAttaching() {
            Message message = new GenericMessage(
                    "message-id",
                    new MessageType("TestCommand"),
                    "payload".getBytes(),
                    Map.of("unrelated", "value")
            );

            Message attached = testSubject.attachTenant(message, TENANT_A);

            assertThat(attached.metadata().get("unrelated")).isEqualTo("value");
            assertThat(attached.metadata().get(DEFAULT_TENANT_METADATA_KEY)).isEqualTo(TENANT_A.tenantId());
        }

        @Test
        void attachedMessageResolvesBackToTheSameTenantThroughTheSameResolverInstance() {
            Message message = new GenericMessage(
                    "message-id",
                    new MessageType("TestCommand"),
                    "payload".getBytes(),
                    Collections.emptyMap()
            );

            Message attached = testSubject.attachTenant(message, TENANT_A);

            assertThat(testSubject.resolveTenant(attached, List.of(TENANT_A))).isSameAs(TENANT_A);
        }
    }
}
