/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.association;

import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class SerializedAssociationTest {

    private final ValueComparisonOperatorRegistry registry = new ValueComparisonOperatorRegistry();

    @Test
    void shouldSerializePayloadRetrieverIntoCanonicalForm() {
        var serialized = SerializedAssociation.from(
                PayloadPropertyValueRetriever.payloadProperty("orderId"),
                "=",
                "123"
        );

        assertThat(serialized.serialize()).isEqualTo("payload:orderId=123");
    }

    @Test
    void shouldRejectLegacyPayloadOnlyAssociationStrings() {
        assertThatThrownBy(() -> SerializedAssociation.parse(registry, "status=vip"))
                .isInstanceOf(BadAssociationFormatException.class)
                .hasMessageContaining("<qualifier>:<path><operator><value>");
    }

    @Test
    void shouldEvaluateCanonicalMetadataAssociation() {
        EventMessage eventMessage = mock(EventMessage.class);
        ProcessingContext processingContext = mock(ProcessingContext.class);
        when(eventMessage.metadata()).thenReturn(Metadata.with("tenantId", "acme"));

        var predicate = SerializedAssociation.parse(registry, "metadata:tenantId=acme")
                                             .asEventMessagePredicate(registry);

        assertThat(predicate.test(eventMessage, processingContext)).isTrue();
    }

    @Test
    void shouldEvaluateCanonicalPayloadAssociation() {
        EventMessage eventMessage = mock(EventMessage.class);
        ProcessingContext processingContext = mock(ProcessingContext.class);
        when(eventMessage.payloadAs(Map.class)).thenReturn(Map.of("status", "vip"));

        var predicate = SerializedAssociation.parse(registry, "payload:status=vip")
                                             .asEventMessagePredicate(registry);

        assertThat(predicate.test(eventMessage, processingContext)).isTrue();
    }

    @Test
    void shouldEvaluateCanonicalPayloadAssociationForNonStringPayloadValue() {
        EventMessage eventMessage = mock(EventMessage.class);
        ProcessingContext processingContext = mock(ProcessingContext.class);
        when(eventMessage.payloadAs(Map.class)).thenReturn(Map.of("amount", 10));

        var predicate = SerializedAssociation.parse(registry, "payload:amount=10")
                                             .asEventMessagePredicate(registry);

        assertThat(predicate.test(eventMessage, processingContext)).isTrue();
    }
}
