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

package org.axonframework.messaging.eventhandling.scheduling.quartz;

import org.axonframework.conversion.TestConverter;
import org.axonframework.messaging.eventhandling.scheduling.quartz.QuartzScheduleToken;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests conversion capabilities of {@link QuartzScheduleToken}.
 *
 * @author JohT
 */
class QuartzScheduleTokenSerializationTest {

    public static Collection<TestConverter> converters() {
       return TestConverter.all();
    }

    @MethodSource("converters")
    @ParameterizedTest
    void tokenShouldBeSerializable(TestConverter converter) {
        QuartzScheduleToken tokenToTest = new QuartzScheduleToken("jobIdentifier", "groupIdentifier");
        assertEquals(tokenToTest, converter.serializeDeserialize(tokenToTest));
    }
}