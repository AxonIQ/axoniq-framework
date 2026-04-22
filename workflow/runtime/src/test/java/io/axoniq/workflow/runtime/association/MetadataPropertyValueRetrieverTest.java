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

import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;
import org.mockito.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

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

        assertThat(result).isEqualTo(expectedValue);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReturnNullWhenMetadataPropertyIsMissing() {
        String propertyName = "missingProperty";
        Metadata metadata = Metadata.with("someOtherProperty", "value");

        ValueRetriever retriever = MetadataPropertyValueRetriever.metadataProperty(propertyName);

        when(eventMessage.metadata()).thenReturn(metadata);

        Object result = retriever.apply(eventMessage, converter);

        assertThat(result).isNull();
    }

    @Test
    void shouldThrowExceptionWhenPropertyNameIsNull() {
        assertThatThrownBy(() -> MetadataPropertyValueRetriever.metadataProperty(null))
                .isInstanceOf(NullPointerException.class);
    }
}
