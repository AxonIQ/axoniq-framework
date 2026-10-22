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

package org.axonframework.messaging.eventhandling.scheduling.java;

import org.axonframework.conversion.TestConverter;
import org.axonframework.messaging.eventhandling.scheduling.java.SimpleScheduleToken;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests conversion capabilities of {@link SimpleScheduleToken}.
 *
 * @author JohT
 */
class SimpleScheduleTokenSerializationTest {

    static Collection<TestConverter> converters() {
       return TestConverter.all();
    }

    @MethodSource("converters")
    @ParameterizedTest
    void tokenShouldBeSerializable(TestConverter converter) {
        SimpleScheduleToken tokenToTest = new SimpleScheduleToken("28bda08d-2dd5-4420-98cb-75ca073446b4");
        assertEquals(tokenToTest, converter.serializeDeserialize(tokenToTest));
    }
}