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

package io.axoniq.framework.kafka.streaming;

import org.apache.kafka.common.TopicPartition;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.*;

class LegacyKafkaTrackingTokenTest {

    private static final TopicPartition P0 = new TopicPartition("parcel-events", 0);
    private static final TopicPartition P1 = new TopicPartition("parcel-events", 1);

    /**
     * A token as the Axon Framework 4 extension serialized it with Jackson: the last consumed offset per partition.
     */
    private static final String AXON_FRAMEWORK_4_JSON =
            "{\"positions\":{\"parcel-events-0\":41,\"parcel-events-1\":9}}";

    private final JacksonConverter converter = new JacksonConverter();

    private LegacyKafkaTrackingToken read(String json) {
        return Objects.requireNonNull(converter.convert(json.getBytes(StandardCharsets.UTF_8),
                                                        LegacyKafkaTrackingToken.class));
    }

    @Test
    void storedLastConsumedOffsetsBecomeNextOffsetsToRead() {
        // when
        LegacyKafkaTrackingToken token = read(AXON_FRAMEWORK_4_JSON);

        // then
        assertThat(token.toKafkaTrackingToken()).isEqualTo(KafkaTrackingToken.of(Map.of(P0, 42L, P1, 10L)));
        assertThat(token.lastConsumedOffsets()).isEqualTo(Map.of(P0, 41L, P1, 9L));
    }

    @Test
    void storingAgainKeepsTheOriginalValues() {
        // given
        LegacyKafkaTrackingToken token = read(AXON_FRAMEWORK_4_JSON);

        // when
        LegacyKafkaTrackingToken reread = converter.convert(converter.convert(token, byte[].class),
                                                            LegacyKafkaTrackingToken.class);

        // then
        assertThat(reread).isEqualTo(token);
        assertThat(reread.toKafkaTrackingToken()).isEqualTo(token.toKafkaTrackingToken());
    }

    @Test
    void comparesWithCurrentTokensThroughItsEquivalent() {
        // given
        LegacyKafkaTrackingToken legacy = read(AXON_FRAMEWORK_4_JSON);
        KafkaTrackingToken next = KafkaTrackingToken.of(Map.of(P0, 43L, P1, 10L));

        // when / then
        assertThat(next.covers(legacy)).isTrue();
        assertThat(legacy.covers(next)).isFalse();
        assertThat(legacy.lowerBound(next)).isEqualTo(legacy.toKafkaTrackingToken());
        assertThat(next.upperBound(legacy)).isEqualTo(next);
        assertThat(legacy.position()).hasValue(52L);
    }

    @Test
    void coversTheFirstTokenAndNull() {
        // given
        LegacyKafkaTrackingToken legacy = read(AXON_FRAMEWORK_4_JSON);

        // when / then
        assertThat(legacy.covers(null)).isTrue();
        assertThat(legacy.covers(TrackingToken.FIRST)).isTrue();
    }

    @Test
    void isAtTheSamePositionAsItsEquivalentWithoutBeingEqualToIt() {
        // given
        LegacyKafkaTrackingToken legacy = read(AXON_FRAMEWORK_4_JSON);
        KafkaTrackingToken equivalent = legacy.toKafkaTrackingToken();

        // when / then
        assertThat(legacy.samePositionAs(equivalent)).isTrue();
        assertThat(equivalent.samePositionAs(legacy)).isTrue();
        assertThat(legacy).isNotEqualTo(equivalent);
        assertThat(equivalent).isNotEqualTo(legacy);
    }

    @Test
    @SuppressWarnings("removal") // the mapper and the interface it implements are a temporary bridge
    void tokenStoresFindTheLegacyTypeThroughTheMapper() {
        // when
        var mappers = ServiceLoader.load(
                org.axonframework.messaging.eventhandling.processing.streaming.token.store.LegacyTokenTypeMapper.class
        );

        // then
        assertThat(mappers.stream().map(ServiceLoader.Provider::get))
                .anySatisfy(mapper -> assertThat(mapper.mappings())
                        .containsEntry(KafkaLegacyTokenTypeMapper.LEGACY_TOKEN_TYPE, LegacyKafkaTrackingToken.class));
    }
}
