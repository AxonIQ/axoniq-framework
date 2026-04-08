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

package org.axonframework.messaging.eventhandling.processing.streaming.token;

import org.axonframework.conversion.TestConverter;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Tests conversion capabilities of {@link MergedTrackingToken}.
 *
 * @author JohT
 */
class MergedTrackingTokenSerializationTest {

    static Collection<TestConverter> converters() {
        return TestConverter.all();
    }

    @MethodSource("converters")
    @ParameterizedTest
    void tokenShouldBeSerializable(TestConverter converter) {
        MergedTrackingToken testSubject = new MergedTrackingToken(new MergedTrackingToken(token(1), token(5)), token(3));
        assertEquals(testSubject, converter.serializeDeserialize(testSubject));
    }

    private GlobalSequenceTrackingToken token(int sequence) {
        return new GlobalSequenceTrackingToken(sequence);
    }
}
