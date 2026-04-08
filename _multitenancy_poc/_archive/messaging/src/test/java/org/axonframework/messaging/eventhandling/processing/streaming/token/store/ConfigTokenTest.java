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

package org.axonframework.messaging.eventhandling.processing.streaming.token.store;

import org.axonframework.conversion.TestConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests conversion capabilities of the {@link ConfigToken}.
 *
 * @author Steven van Beelen
 */
class ConfigTokenTest {

    static Collection<TestConverter> converters() {
        return TestConverter.all();
    }

    @MethodSource("converters")
    @ParameterizedTest
    void tokenShouldBeSerializable(TestConverter converter) {
        Map<String, String> configMap = Collections.singletonMap("some-key", "some-value");
        ConfigToken token = new ConfigToken(configMap);
        assertEquals(token, converter.serializeDeserialize(token));
    }

    @Test
    void samePositionAsUnsupportedOperationException() {
        Map<String, String> configMap = Collections.singletonMap("some-key", "some-value");
        ConfigToken token = new ConfigToken(configMap);

        assertThrows(UnsupportedOperationException.class, () -> token.samePositionAs(token));
        assertThrows(UnsupportedOperationException.class,
                     () -> token.samePositionAs(new GlobalSequenceTrackingToken(0)));
        assertThrows(UnsupportedOperationException.class, () -> token.samePositionAs(null));
    }
}