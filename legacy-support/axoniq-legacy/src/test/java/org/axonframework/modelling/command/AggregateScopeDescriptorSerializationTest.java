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

package org.axonframework.modelling.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson2.Jackson2Converter;
import org.axonframework.modelling.OnlyAcceptConstructorPropertiesAnnotation;
import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests conversion capabilities of {@link AggregateScopeDescriptor}.
 */
class AggregateScopeDescriptorSerializationTest {

    private final String expectedType = "aggregateType";
    private final String expectedIdentifier = "identifier";

    private AggregateScopeDescriptor testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new AggregateScopeDescriptor(expectedType, expectedIdentifier);
    }

    @Test
    void jacksonSerializationWorksAsExpected() {
        // given
        Converter converter = new Jackson2Converter();

        // when
        byte[] serialized = converter.convert(testSubject, byte[].class);
        AggregateScopeDescriptor result = converter.convert(serialized, AggregateScopeDescriptor.class);

        // then
        assertThat(result.getType()).isEqualTo(expectedType);
        assertThat(result.getIdentifier()).isEqualTo(expectedIdentifier);
    }

    @Test
    void jacksonSerializationWorksUsingOnlyConstructorProperties() {
        // given
        ObjectMapper objectMapper = OnlyAcceptConstructorPropertiesAnnotation.attachTo(new ObjectMapper());
        Converter converter = new Jackson2Converter(objectMapper);

        // when
        byte[] serialized = converter.convert(testSubject, byte[].class);
        AggregateScopeDescriptor result = converter.convert(serialized, AggregateScopeDescriptor.class);

        // then
        assertThat(result.getType()).isEqualTo(expectedType);
        assertThat(result.getIdentifier()).isEqualTo(expectedIdentifier);
    }

    @Test
    void lazyIdentifierSupplierIsResolvedOnlyOnceOnFirstAccess() {
        // given
        AtomicInteger supplierInvocations = new AtomicInteger();
        AggregateScopeDescriptor lazyDescriptor = new AggregateScopeDescriptor(expectedType, () -> {
            supplierInvocations.incrementAndGet();
            return expectedIdentifier;
        });
        // nothing is resolved while constructing the descriptor
        assertThat(supplierInvocations).hasValue(0);

        // when
        Object firstAccess = lazyDescriptor.getIdentifier();
        Object secondAccess = lazyDescriptor.getIdentifier();

        // then
        assertThat(firstAccess).isEqualTo(expectedIdentifier);
        assertThat(secondAccess).isEqualTo(expectedIdentifier);
        assertThat(supplierInvocations).hasValue(1);
    }
}
