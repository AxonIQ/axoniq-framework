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
package io.axoniq.framework.workflow.dsl.api;

import io.axoniq.framework.workflow.runtime.association.Associations;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EventConditionsTest {

    @Test
    void shouldEvaluateSingleAssociationPredicate() {
        var condition = EventConditions.fromQualifiedName(
                new QualifiedName("ns.Event"),
                Associations.associate(payloadProperty("orderId"), "=", "123")
        );
        var processingContext = mock(ProcessingContext.class);
        var eventMessage = mock(EventMessage.class);
        when(eventMessage.payloadAs(Map.class)).thenReturn(Map.of("orderId", "123"));

        assertThat(condition.predicate().test(eventMessage, processingContext)).isTrue();
    }

    @Test
    void shouldEvaluateMultipleAssociationsPredicate() {
        var condition = EventConditions.fromQualifiedName(
                new QualifiedName("ns.Event"),
                Associations.associate(payloadProperty("orderId"), "=", "123")
                            .and(payloadProperty("customerId"), "=", "abc")
        );
        var processingContext = mock(ProcessingContext.class);
        var matchingEvent = mock(EventMessage.class);
        when(matchingEvent.payloadAs(Map.class)).thenReturn(Map.of("orderId", "123", "customerId", "abc"));

        var partialEvent = mock(EventMessage.class);
        when(partialEvent.payloadAs(Map.class)).thenReturn(Map.of("orderId", "123"));

        assertThat(condition.predicate().test(matchingEvent, processingContext)).isTrue();
        assertThat(condition.predicate().test(partialEvent, processingContext)).isFalse();
    }

    @Test
    void shouldEvaluateMetadataAssociationsPredicate() {
        var condition = EventConditions.fromQualifiedName(
                new QualifiedName("ns.Event"),
                Associations.parse(new io.axoniq.framework.workflow.runtime.association.ValueComparisonOperatorRegistry(),
                                   "metadata:tenantId=acme")
        );
        var processingContext = mock(ProcessingContext.class);
        var eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.with("tenantId", "acme"));

        assertThat(condition.predicate().test(eventMessage, processingContext)).isTrue();
    }
}
