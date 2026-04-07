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
package io.axoniq.workflow.runtime.association;

import io.axoniq.workflow.runtime.association.MetadataPropertyValueRetriever;
import io.axoniq.workflow.runtime.association.ValueRetriever;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link MetadataPropertyValueRetriever}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class MetadataPropertyValueRetrieverTest {

    private final Converter converter = Mockito.mock(Converter.class);
    private final EventMessage eventMessage = Mockito.mock(EventMessage.class);

    @BeforeEach
    void setUp() {
        Mockito.reset(converter, eventMessage);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldRetrieveValueFromMetadata() {
        String propertyName = "orderId";
        String expectedValue = "12345";
        Metadata metadata = Metadata.with(propertyName, expectedValue);

        ValueRetriever retriever = MetadataPropertyValueRetriever.metadataProperty(propertyName);

        when(eventMessage.metadata()).thenReturn(metadata);

        Object result = retriever.apply(eventMessage, converter);

        assertEquals(expectedValue, result);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReturnNullWhenMetadataPropertyIsMissing() {
        String propertyName = "missingProperty";
        Metadata metadata = Metadata.with("someOtherProperty", "value");

        ValueRetriever retriever = MetadataPropertyValueRetriever.metadataProperty(propertyName);

        when(eventMessage.metadata()).thenReturn(metadata);

        Object result = retriever.apply(eventMessage, converter);

        assertNull(result);
    }

    @Test
    void shouldThrowExceptionWhenPropertyNameIsNull() {
        assertThrows(NullPointerException.class, () -> MetadataPropertyValueRetriever.metadataProperty(null));
    }
}
