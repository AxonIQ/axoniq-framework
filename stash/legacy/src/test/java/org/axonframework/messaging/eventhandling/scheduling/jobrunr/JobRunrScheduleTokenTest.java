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

package org.axonframework.messaging.eventhandling.scheduling.jobrunr;

import org.axonframework.messaging.eventhandling.scheduling.ScheduleToken;
import org.axonframework.conversion.TestConverter;
import org.axonframework.messaging.eventhandling.scheduling.jobrunr.JobRunrScheduleToken;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.util.Collection;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class JobRunrScheduleTokenTest {

    @Test
    void equalsIfSameUuidIsUsed() {
        UUID uuid = UUID.randomUUID();
        ScheduleToken one = new JobRunrScheduleToken(uuid);
        ScheduleToken other = new JobRunrScheduleToken(uuid);

        assertEquals(one, other);
        assertEquals(one.toString(), other.toString());
        assertEquals(one.hashCode(), other.hashCode());
    }

    @Test
    void notEqualsIfDifferentUuidIsUsed() {
        ScheduleToken one = new JobRunrScheduleToken(UUID.randomUUID());
        ScheduleToken other = new JobRunrScheduleToken(UUID.randomUUID());

        assertNotEquals(one, other);
        assertNotEquals(one.toString(), other.toString());
        assertNotEquals(one.hashCode(), other.hashCode());
    }

    @MethodSource("converters")
    @ParameterizedTest
    void tokenShouldBeSerializable(TestConverter converter) {
        ScheduleToken tokenToTest = new JobRunrScheduleToken(UUID.randomUUID());
        assertEquals(tokenToTest, converter.serializeDeserialize(tokenToTest));
    }

    public static Collection<TestConverter> converters() {
        return TestConverter.all();
    }
}
