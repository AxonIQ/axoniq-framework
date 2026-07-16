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

package io.axoniq.framework.messaging.eventstreaming;

import org.axonframework.conversion.Converter;
import org.axonframework.conversion.TestConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.LegacyTokenTypes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * Validates that an Axon Framework 4 {@link MultiSourceTrackingToken} can be read after upgrading, by registering the
 * class name mapping through {@link org.axonframework.messaging.eventhandling.processing.streaming.token.store.LegacyTokenTypeMapper}.
 */
@SuppressWarnings("removal") // exercises the deprecated migration bridge on purpose
class MultiSourceLegacyTokenTypeMapperTest {

    private static final String AXON_4_MULTI_SOURCE = "org.axonframework.eventhandling.MultiSourceTrackingToken";

    @Nested
    class Mapping {

        @Test
        void mapsAxon4NameToCurrentClass() {
            assertThat(new MultiSourceLegacyTokenTypeMapper().mappings())
                    .containsExactly(entry(AXON_4_MULTI_SOURCE, MultiSourceTrackingToken.class));
        }

        @Test
        void mappingIsDiscoveredThroughServiceLoader() {
            assertThat(LegacyTokenTypes.currentTypeFor(AXON_4_MULTI_SOURCE)).isEqualTo(MultiSourceTrackingToken.class);
        }
    }

    @Nested
    class Deserialization {

        @Test
        void axon4MultiSourceTokenIsRead() {
            Converter converter = TestConverter.JACKSON.getConverter();
            byte[] axon4Json = """
                    {"trackingTokens":{\
                    "streamA":{"@c":".GlobalSequenceTrackingToken","globalIndex":5},\
                    "streamB":{"@c":".GlobalSequenceTrackingToken","globalIndex":9}}}""".getBytes(StandardCharsets.UTF_8);

            TrackingToken result = LegacyTokenTypes.deserialize(converter, axon4Json, AXON_4_MULTI_SOURCE);

            assertThat(result).isInstanceOf(MultiSourceTrackingToken.class);
            Map<String, TrackingToken> streamTokens = ((MultiSourceTrackingToken) result).getTrackingTokens();
            assertThat(streamTokens).containsOnlyKeys("streamA", "streamB");
            assertThat(streamTokens.get("streamA").position()).hasValue(5L);
            assertThat(streamTokens.get("streamB").position()).hasValue(9L);
        }
    }
}
