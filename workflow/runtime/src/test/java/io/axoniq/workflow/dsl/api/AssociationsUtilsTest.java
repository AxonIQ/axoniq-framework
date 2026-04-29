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
package io.axoniq.workflow.dsl.api;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link AssociationsUtils}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class AssociationsUtilsTest {

    private EventConverter converter;

    @BeforeEach
    void setUp() {
        converter = Mockito.mock(EventConverter.class);
    }

    @Test
    void testBuildSingleAssociation() {
        Predicate<EventMessage> predicate = AssociationsUtils
                .associate(payloadProperty("orderId"), "=", "123")
                .build(converter);

        Map<String, Object> payload = Map.of("orderId", "123");
        EventMessage eventMessage = Mockito.mock(EventMessage.class);
        when(eventMessage.payloadAs(any(Type.class), eq(converter))).thenReturn(payload);

        assertThat(predicate.test(eventMessage)).isTrue();
    }

    @Test
    void testBuildMultipleAssociations() {
        Predicate<EventMessage> predicate = AssociationsUtils
                .associate(payloadProperty("orderId"), "=", "123")
                .and(payloadProperty("customerId"), "=", "abc")
                .build(converter);

        Map<String, Object> payload = Map.of("orderId", "123", "customerId", "abc");
        EventMessage eventMessage = Mockito.mock(EventMessage.class);
        when(eventMessage.payloadAs(any(Type.class), eq(converter))).thenReturn(payload);

        assertThat(predicate.test(eventMessage)).isTrue();

        Map<String, Object> partialPayload = Map.of("orderId", "123");
        EventMessage partialEventMessage = Mockito.mock(EventMessage.class);
        when(partialEventMessage.payloadAs(any(Type.class), eq(converter))).thenReturn(partialPayload);

        assertThat(predicate.test(partialEventMessage)).isFalse();

        Map<String, Object> mismatchPayload = Map.of("orderId", "123", "customerId", "wrong");
        EventMessage mismatchEventMessage = Mockito.mock(EventMessage.class);
        when(mismatchEventMessage.payloadAs(any(Type.class), eq(converter))).thenReturn(mismatchPayload);

        assertThat(predicate.test(mismatchEventMessage)).isFalse();
    }

    @Test
    void testBuildWithProcessingContext() {
        ProcessingContext processingContext = Mockito.mock(ProcessingContext.class);
        when(processingContext.component(EventConverter.class)).thenReturn(converter);

        Predicate<EventMessage> predicate = AssociationsUtils
                .associate(payloadProperty("orderId"), "=", "123")
                .build(processingContext);

        Map<String, Object> payload = Map.of("orderId", "123");
        EventMessage eventMessage = Mockito.mock(EventMessage.class);
        when(eventMessage.payloadAs(any(Type.class), eq(converter))).thenReturn(payload);

        assertThat(predicate.test(eventMessage)).isTrue();
    }

    @Test
    void testBuildWithMissingKey() {
        Predicate<EventMessage> predicate = AssociationsUtils
                .associate(payloadProperty("orderId"), "=", "123")
                .build(converter);

        Map<String, Object> payload = Map.of("somethingElse", "123");
        EventMessage eventMessage = Mockito.mock(EventMessage.class);
        when(eventMessage.payloadAs(any(Type.class), eq(converter))).thenReturn(payload);

        assertThat(predicate.test(eventMessage)).isFalse();
    }
}
