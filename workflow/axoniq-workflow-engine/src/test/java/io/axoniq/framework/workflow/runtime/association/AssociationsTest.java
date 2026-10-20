/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.association;

import org.junit.jupiter.api.*;

import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link Associations}.
 *
 * @author Simon Zambrovski
 */
class AssociationsTest {

    @Test
    void shouldSerializePayloadAssociationsIntoCanonicalQualifiedForm() {
        var associations = Associations.associate(payloadProperty("orderId"), "=", "123")
                                       .and(payloadProperty("customerId"), "=", "abc");

        assertThat(associations.criteria())
                .containsExactlyInAnyOrder("payload:orderId=123", "payload:customerId=abc");
    }

    @Test
    void shouldReturnAssociations() {
        var associations = Associations.associate(payloadProperty("z"), "=", "1")
                                       .and(payloadProperty("a"), "=", "2")
                                       .and(payloadProperty("m"), "=", "3");

        assertThat(associations.criteria())
                .containsExactlyInAnyOrder("payload:a=2", "payload:m=3", "payload:z=1");
    }

    @Test
    void shouldRejectLegacyPayloadStringsWhenParsingAssociations() {
        assertThatThrownBy(() -> Associations.parse(new ValueComparisonOperatorRegistry(), "status=vip"))
                .isInstanceOf(io.axoniq.framework.workflow.runtime.association.BadAssociationFormatException.class)
                .hasMessageContaining("<qualifier>:<path><operator><value>");
    }

    @Test
    void shouldParseMetadataAssociationsInCanonicalForm() {
        var associations = Associations.parse(new ValueComparisonOperatorRegistry(), "metadata:tenantId=acme");

        assertThat(associations.criteria()).containsExactly("metadata:tenantId=acme");
    }
}
