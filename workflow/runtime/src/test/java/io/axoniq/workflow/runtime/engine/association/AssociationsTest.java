/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.association;

import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.Map;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link Associations}.
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class AssociationsTest {

    private Converter converter;

    @BeforeEach
    void setUp() {
        converter = Mockito.mock(Converter.class);
    }

    @Test
    void testBuildSingleAssociation() {
        Predicate<EventMessage> predicate = Associations
                .associate("orderId", "=", "123")
                .build(converter);

        Map<String, Object> payload = Map.of("orderId", "123");
        EventMessage eventMessage = Mockito.mock(EventMessage.class);
        when(eventMessage.payloadAs(any(TypeReference.class), eq(converter))).thenReturn(payload);

        assertTrue(predicate.test(eventMessage));
    }

    @Test
    void testBuildMultipleAssociations() {
        Predicate<EventMessage> predicate = Associations
                .associate("orderId", "=", "123")
                .and("customerId", "=", "abc")
                .build(converter);

        Map<String, Object> payload = Map.of("orderId", "123", "customerId", "abc");
        EventMessage eventMessage = Mockito.mock(EventMessage.class);
        when(eventMessage.payloadAs(any(TypeReference.class), eq(converter))).thenReturn(payload);

        assertTrue(predicate.test(eventMessage));

        Map<String, Object> partialPayload = Map.of("orderId", "123");
        EventMessage partialEventMessage = Mockito.mock(EventMessage.class);
        when(partialEventMessage.payloadAs(any(TypeReference.class), eq(converter))).thenReturn(partialPayload);

        assertFalse(predicate.test(partialEventMessage));

        Map<String, Object> mismatchPayload = Map.of("orderId", "123", "customerId", "wrong");
        EventMessage mismatchEventMessage = Mockito.mock(EventMessage.class);
        when(mismatchEventMessage.payloadAs(any(TypeReference.class), eq(converter))).thenReturn(mismatchPayload);

        assertFalse(predicate.test(mismatchEventMessage));
    }

    @Test
    void testBuildWithProcessingContext() {
        ProcessingContext processingContext = Mockito.mock(ProcessingContext.class);
        when(processingContext.component(Converter.class)).thenReturn(converter);

        Predicate<EventMessage> predicate = Associations
                .associate("orderId", "=", "123")
                .build(processingContext);

        Map<String, Object> payload = Map.of("orderId", "123");
        EventMessage eventMessage = Mockito.mock(EventMessage.class);
        when(eventMessage.payloadAs(any(TypeReference.class), eq(converter))).thenReturn(payload);

        assertTrue(predicate.test(eventMessage));
    }

    @Test
    void testBuildWithMissingKey() {
        Predicate<EventMessage> predicate = Associations
                .associate("orderId", "=", "123")
                .build(converter);

        Map<String, Object> payload = Map.of("somethingElse", "123");
        EventMessage eventMessage = Mockito.mock(EventMessage.class);
        when(eventMessage.payloadAs(any(TypeReference.class), eq(converter))).thenReturn(payload);

        assertFalse(predicate.test(eventMessage));
    }
}
