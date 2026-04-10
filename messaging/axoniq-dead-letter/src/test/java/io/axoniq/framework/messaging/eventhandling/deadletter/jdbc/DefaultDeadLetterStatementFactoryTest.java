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

package io.axoniq.framework.messaging.eventhandling.deadletter.jdbc;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link DefaultDeadLetterStatementFactory}.
 *
 * @author Steven van Beelen
 */
class DefaultDeadLetterStatementFactoryTest {

    @Test
    void buildWithNullSchemaThrowsAxonConfigurationException() {
        DefaultDeadLetterStatementFactory.Builder<?> testBuilder = DefaultDeadLetterStatementFactory.builder();

        assertThrows(AxonConfigurationException.class, () -> testBuilder.schema(null));
    }

    @Test
    void buildWithNullGenericConverterThrowsAxonConfigurationException() {
        DefaultDeadLetterStatementFactory.Builder<?> testBuilder = DefaultDeadLetterStatementFactory.builder();

        assertThrows(AxonConfigurationException.class, () -> testBuilder.genericConverter(null));
    }

    @Test
    void buildWithNullEventConverterThrowsAxonConfigurationException() {
        DefaultDeadLetterStatementFactory.Builder<?> testBuilder = DefaultDeadLetterStatementFactory.builder();

        assertThrows(AxonConfigurationException.class, () -> testBuilder.eventConverter(null));
    }

    @Test
    void buildWithoutTheGenericConverterThrowsAxonConfigurationException() {
        JacksonConverter jacksonConverter = new JacksonConverter();
        DefaultDeadLetterStatementFactory.Builder<?> testBuilder =
                DefaultDeadLetterStatementFactory.builder()
                                                 .eventConverter(new DelegatingEventConverter(jacksonConverter));

        assertThrows(AxonConfigurationException.class, testBuilder::build);
    }

    @Test
    void buildWithoutTheEventConverterThrowsAxonConfigurationException() {
        DefaultDeadLetterStatementFactory.Builder<?> testBuilder =
                DefaultDeadLetterStatementFactory.builder()
                                                 .genericConverter(new JacksonConverter());

        assertThrows(AxonConfigurationException.class, testBuilder::build);
    }
}
