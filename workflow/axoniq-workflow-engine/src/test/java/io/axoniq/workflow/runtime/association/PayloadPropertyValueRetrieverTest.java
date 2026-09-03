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

import org.jspecify.annotations.Nullable;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link PayloadPropertyValueRetriever}.
 *
 * @author Simon Zambrovski
 */
class PayloadPropertyValueRetrieverTest {

    private final EventMessage eventMessage = Mockito.mock(EventMessage.class);
    private final ProcessingContext pc = Mockito.mock(ProcessingContext.class);

    @BeforeEach
    void setUp() {
        Mockito.reset(eventMessage);
    }

    @Test
    void shouldRetrieveValueFromPayload() {
        String propertyName = "orderId";
        String expectedValue = "12345";
        Map<String, @Nullable Object> payload = new HashMap<>();
        payload.put(propertyName, expectedValue);

        ValueRetriever retriever = PayloadPropertyValueRetriever.payloadProperty(propertyName);

        when(eventMessage.payloadAs(Map.class)).thenReturn(payload);

        Object result = retriever.apply(eventMessage, pc);

        assertThat(result).isEqualTo(expectedValue);
        assertThat(retriever.qualifier()).isEqualTo(PayloadPropertyValueRetriever.QUALIFIER);
        assertThat(retriever.path()).isEqualTo(propertyName);
        verify(eventMessage).payloadAs(Map.class);
    }

    @Test
    void shouldReturnNullWhenPropertyIsMissing() {
        String propertyName = "missingProperty";
        Map<String, @Nullable Object> payload = new HashMap<>();
        payload.put("someOtherProperty", "value");

        ValueRetriever retriever = PayloadPropertyValueRetriever.payloadProperty(propertyName);

        when(eventMessage.payloadAs(Map.class)).thenReturn(payload);

        Object result = retriever.apply(eventMessage, pc);

        assertThat(result).isNull();
    }

    @Test
    void shouldThrowExceptionWhenPayloadIsNull() {
        String propertyName = "orderId";
        ValueRetriever retriever = PayloadPropertyValueRetriever.payloadProperty(propertyName);

        when(eventMessage.payloadAs(Map.class)).thenReturn(null);

        assertThatThrownBy(() -> retriever.apply(eventMessage, pc))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowExceptionWhenPropertyNameIsNull() {
        assertThatThrownBy(() -> PayloadPropertyValueRetriever.payloadProperty(null))
                .isInstanceOf(NullPointerException.class);
    }
}
