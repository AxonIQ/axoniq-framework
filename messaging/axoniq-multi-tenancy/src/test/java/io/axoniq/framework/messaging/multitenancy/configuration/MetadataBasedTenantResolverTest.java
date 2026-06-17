/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
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

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataBasedTenantResolverTest {

    private static final TenantDescriptor TENANT_A = new TenantDescriptor(
            "foo-a",
            Map.of("replicationGroup", "rg-a")
    );

    @Test
    void resolvesRegisteredTenantDescriptorWhenPropertiesDiffer() {
        MetadataBasedTenantResolver testSubject = new MetadataBasedTenantResolver();
        Message message = new GenericMessage(
                "message-id",
                new MessageType("TestCommand"),
                "payload".getBytes(),
                Map.of(MetadataBasedTenantResolver.DEFAULT_TENANT_KEY, TENANT_A.tenantId())
        );

        TenantDescriptor resolved = testSubject.apply(message, List.of(TENANT_A));

        assertThat(resolved).isSameAs(TENANT_A);
    }

    @Test
    void fallsBackToTenantIdOnlyDescriptorWhenTenantIsUnknown() {
        MetadataBasedTenantResolver testSubject = new MetadataBasedTenantResolver();
        Message message = new GenericMessage(
                "message-id",
                new MessageType("TestCommand"),
                "payload".getBytes(),
                Map.of(MetadataBasedTenantResolver.DEFAULT_TENANT_KEY, "foo-b")
        );

        TenantDescriptor resolved = testSubject.apply(message, List.of(TENANT_A));

        assertThat(resolved).isEqualTo(TenantDescriptor.tenantWithId("foo-b"));
    }
}
