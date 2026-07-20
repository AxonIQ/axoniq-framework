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
}
