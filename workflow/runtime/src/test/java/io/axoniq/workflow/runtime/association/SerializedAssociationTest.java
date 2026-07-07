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

import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
}
