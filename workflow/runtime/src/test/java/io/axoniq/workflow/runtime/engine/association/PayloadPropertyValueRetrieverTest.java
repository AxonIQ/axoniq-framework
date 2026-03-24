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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.engine.association;

import org.axonframework.conversion.Converter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PayloadPropertyValueRetriever}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class PayloadPropertyValueRetrieverTest {

    private final Converter converter = Mockito.mock(Converter.class);
    private final EventMessage eventMessage = Mockito.mock(EventMessage.class);

    @BeforeEach
    void setUp() {
        Mockito.reset(converter, eventMessage);
    }

    @Test
    void shouldRetrieveValueFromPayload() {
        String propertyName = "orderId";
        String expectedValue = "12345";
        Map<String, Object> payload = new HashMap<>();
        payload.put(propertyName, expectedValue);

        ValueRetriever retriever = PayloadPropertyValueRetriever.payloadProperty(propertyName);

        when(eventMessage.payloadAs(any(java.lang.reflect.Type.class), any())).thenReturn(payload);

        Object result = retriever.apply(eventMessage, converter);

        assertEquals(expectedValue, result);
        verify(eventMessage).payloadAs(any(java.lang.reflect.Type.class), any());
    }

    @Test
    void shouldReturnNullWhenPropertyIsMissing() {
        String propertyName = "missingProperty";
        Map<String, Object> payload = new HashMap<>();
        payload.put("someOtherProperty", "value");

        ValueRetriever retriever = PayloadPropertyValueRetriever.payloadProperty(propertyName);

        when(eventMessage.payloadAs(any(java.lang.reflect.Type.class), any())).thenReturn(payload);

        Object result = retriever.apply(eventMessage, converter);

        assertNull(result);
    }

    @Test
    void shouldThrowExceptionWhenPayloadIsNull() {
        String propertyName = "orderId";
        ValueRetriever retriever = PayloadPropertyValueRetriever.payloadProperty(propertyName);

        when(eventMessage.payloadAs(any(java.lang.reflect.Type.class), any())).thenReturn(null);

        assertThrows(NullPointerException.class, () -> retriever.apply(eventMessage, converter));
    }

    @Test
    void shouldThrowExceptionWhenPropertyNameIsNull() {
        assertThrows(NullPointerException.class, () -> PayloadPropertyValueRetriever.payloadProperty(null));
    }
}
