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
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests conversion capabilities of {@link ReplayToken}.
 *
 * @author JohT
 */
class ReplayTokenConversionTest {

    static Collection<TestConverter> converters() {
        return TestConverter.all();
    }

    @MethodSource("converters")
    @ParameterizedTest
    void tokenShouldBeConverted(TestConverter converter) {
        // given...
        TrackingToken innerToken = GapAwareTrackingToken.newInstance(10, Collections.singleton(9L));
        TrackingToken expected = ReplayToken.createReplayToken(innerToken);
        // when...
        TrackingToken actual = converter.serializeDeserialize(expected);
        // then...
        assertThat(actual).isEqualTo(expected);
    }

    @MethodSource("converters")
    @ParameterizedTest
    void tokenWithContextShouldBeConverted(TestConverter converter) {
        // given...
        TrackingToken innerToken = GapAwareTrackingToken.newInstance(10, Collections.singleton(9L));
        byte[] expectedContext = converter.getConverter().convert("test", byte[].class);
        TrackingToken expected = ReplayToken.createReplayToken(innerToken, null, expectedContext);
        // when...
        TrackingToken actual = converter.serializeDeserialize(expected);
        // then...
        assertThat(actual).isEqualTo(expected);
    }
}