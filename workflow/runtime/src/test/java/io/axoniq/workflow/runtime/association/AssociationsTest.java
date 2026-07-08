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
package io.axoniq.workflow.runtime.association;

import org.junit.jupiter.api.*;

import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link Associations}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class AssociationsTest {

    @Test
    void shouldSerializePayloadAssociationsIntoCanonicalQualifiedForm() {
        var associations = Associations.associate(payloadProperty("orderId"), "=", "123")
                                           .and(payloadProperty("customerId"), "=", "abc");

        assertThat(associations.serializedAssociations())
                .containsExactly("payload:orderId=123", "payload:customerId=abc");
    }

    @Test
    void shouldRejectLegacyPayloadStringsWhenParsingAssociations() {
        assertThatThrownBy(() -> Associations.parse(new ValueComparisonOperatorRegistry(), "status=vip"))
                .isInstanceOf(io.axoniq.workflow.runtime.association.BadAssociationFormatException.class)
                .hasMessageContaining("<qualifier>:<path><operator><value>");
    }

    @Test
    void shouldParseMetadataAssociationsInCanonicalForm() {
        var associations = Associations.parse(new ValueComparisonOperatorRegistry(), "metadata:tenantId=acme");

        assertThat(associations.serializedAssociations()).containsExactly("metadata:tenantId=acme");
    }
}
