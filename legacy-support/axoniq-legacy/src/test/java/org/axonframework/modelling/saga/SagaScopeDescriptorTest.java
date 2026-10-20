/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.modelling.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson2.Jackson2Converter;
import org.axonframework.modelling.OnlyAcceptConstructorPropertiesAnnotation;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the {@link SagaScopeDescriptor}: its description and its conversion.
 *
 * @author JohT
 */
class SagaScopeDescriptorTest {

    private final String expectedType = "sagaType";
    private final String expectedIdentifier = "identifier";

    private SagaScopeDescriptor testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new SagaScopeDescriptor(expectedType, expectedIdentifier);
    }

    @Test
    void jacksonSerializationWorksAsExpected() {
        Converter converter = new Jackson2Converter();

        byte[] serialized = converter.convert(testSubject, byte[].class);
        SagaScopeDescriptor result = converter.convert(serialized, SagaScopeDescriptor.class);

        assertThat(result.getType()).isEqualTo(expectedType);
        assertThat(result.getIdentifier()).isEqualTo(expectedIdentifier);
    }

    @Test
    void responseTypeShouldBeSerializableWithJacksonUsingConstructorProperties() {
        ObjectMapper objectMapper = OnlyAcceptConstructorPropertiesAnnotation.attachTo(new ObjectMapper());
        Converter converter = new Jackson2Converter(objectMapper);

        byte[] serialized = converter.convert(testSubject, byte[].class);
        SagaScopeDescriptor result = converter.convert(serialized, SagaScopeDescriptor.class);

        assertThat(result.getType()).isEqualTo(expectedType);
        assertThat(result.getIdentifier()).isEqualTo(expectedIdentifier);
    }

    @Test
    void scopeDescriptionNamesTheTypeAndIdentifier() {
        // when
        String result = testSubject.scopeDescription();

        // then
        assertThat(result).isEqualTo("SagaScopeDescriptor for type [sagaType] and identifier [identifier]");
    }
}
