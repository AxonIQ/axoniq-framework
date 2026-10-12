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
import org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class KafkaTrackingTokenTest {

    private static final TopicPartition P0 = new TopicPartition("parcels", 0);
    private static final TopicPartition P1 = new TopicPartition("parcels", 1);
    private static final TopicPartition DASHED = new TopicPartition("parcel-events", 10);

    @Nested
    class Bounds {

        @Test
        void lowerBoundTakesMinimumPerPartitionLeavingOutPartitionsEitherTokenDoesNotMention() {
            // given
            KafkaTrackingToken a = KafkaTrackingToken.of(Map.of(P0, 5L, P1, 2L));
            KafkaTrackingToken b = KafkaTrackingToken.of(Map.of(P0, 3L));

            // when
            TrackingToken result = a.lowerBound(b);

            // then - the second token reads partition 1 from its earliest available record, whatever its offset
            assertThat(result).isEqualTo(KafkaTrackingToken.of(Map.of(P0, 3L)));
            assertThat(a.covers(result)).isTrue();
            assertThat(b.covers(result)).isTrue();
        }

        @Test
        void upperBoundKeepsPartitionsThatOneTokenDoesNotMention() {
            // given
            KafkaTrackingToken a = KafkaTrackingToken.of(Map.of(P0, 5L, P1, 2L));
            KafkaTrackingToken b = KafkaTrackingToken.of(Map.of(P0, 3L));

            // when
            TrackingToken result = a.upperBound(b);

            // then
            assertThat(result).isEqualTo(KafkaTrackingToken.of(Map.of(P0, 5L, P1, 2L)));
        }

        @Test
        void upperBoundTakesMaximumPerPartition() {
            // given
            KafkaTrackingToken a = KafkaTrackingToken.of(Map.of(P0, 5L, P1, 2L));
            KafkaTrackingToken b = KafkaTrackingToken.of(Map.of(P0, 3L, P1, 7L));

            // when
            TrackingToken result = a.upperBound(b);

            // then
            assertThat(result).isEqualTo(KafkaTrackingToken.of(Map.of(P0, 5L, P1, 7L)));
        }

        @Test
        void boundsWithFirstTokenDelegateToIt() {
            // given
            KafkaTrackingToken token = KafkaTrackingToken.of(Map.of(P0, 5L));

            // when / then
            assertThat(token.lowerBound(TrackingToken.FIRST)).isEqualTo(TrackingToken.FIRST);
            assertThat(token.upperBound(TrackingToken.FIRST)).isEqualTo(token);
        }

        @Test
        void boundsWithLatestTokenDelegateToIt() {
            // given
            KafkaTrackingToken token = KafkaTrackingToken.of(Map.of(P0, 5L));

            // when / then
            assertThat(token.lowerBound(TrackingToken.LATEST)).isEqualTo(token);
            assertThat(token.upperBound(TrackingToken.LATEST)).isEqualTo(TrackingToken.LATEST);
        }

        @Test
        void boundsWithATokenOfAnotherTypeAreRejected() {
            // given - a foreign token that would delegate back, as the Kafka token once did
            KafkaTrackingToken token = KafkaTrackingToken.of(Map.of(P0, 5L));
            TrackingToken foreign = new DelegatingToken();

            // when / then
            assertThatThrownBy(() -> token.lowerBound(foreign)).isInstanceOf(IllegalArgumentException.class)
                                                               .hasMessageContaining("DelegatingToken");
            assertThatThrownBy(() -> token.upperBound(foreign)).isInstanceOf(IllegalArgumentException.class)
                                                               .hasMessageContaining("DelegatingToken");
        }
    }

    /**
     * A token of another type that leaves bounds with tokens of other types to those tokens.
     */
    private static final class DelegatingToken implements TrackingToken {

        @Override
        public TrackingToken lowerBound(TrackingToken other) {
            return other.lowerBound(this);
        }

        @Override
        public TrackingToken upperBound(TrackingToken other) {
            return other.upperBound(this);
        }

        @Override
        public boolean covers(@Nullable TrackingToken other) {
            return false;
        }
    }

    @Nested
    class Covers {

        @Test
        void coversWhenAtOrBeyondInEveryPartition() {
            // given
            KafkaTrackingToken ahead = KafkaTrackingToken.of(Map.of(P0, 5L, P1, 2L));

            // when / then
            assertThat(ahead.covers(KafkaTrackingToken.of(Map.of(P0, 5L)))).isTrue();
            assertThat(ahead.covers(KafkaTrackingToken.of(Map.of(P0, 6L)))).isFalse();
            assertThat(ahead.covers(KafkaTrackingToken.of(Map.of(DASHED, 1L)))).isFalse();
            assertThat(ahead.covers(KafkaTrackingToken.empty())).isTrue();
        }

        @Test
        void coversFirstTokenAndNull() {
            // given
            KafkaTrackingToken token = KafkaTrackingToken.empty();

            // when / then
            assertThat(token.covers(TrackingToken.FIRST)).isTrue();
            assertThat(token.covers(null)).isTrue();
        }

        @Test
        void neverCoversATokenOfAnotherType() {
            // given
            KafkaTrackingToken token = KafkaTrackingToken.of(Map.of(P0, 5L));

            // when / then
            assertThat(token.covers(new DelegatingToken())).isFalse();
        }
    }

    @Nested
    class Positions {

        @Test
        void advancedToMovesOnePartitionOnly() {
            // given
            KafkaTrackingToken token = KafkaTrackingToken.of(Map.of(P0, 5L, P1, 2L));

            // when
            KafkaTrackingToken result = token.advancedTo(P1, 3L);

            // then
            assertThat(result.positions()).isEqualTo(Map.of(P0, 5L, P1, 3L));
            assertThat(token.offsetOf(P1)).isEqualTo(2L);
        }

        @Test
        void positionIsSumOfOffsets() {
            // when / then
            assertThat(KafkaTrackingToken.of(Map.of(P0, 5L, P1, 2L)).position()).hasValue(7L);
            assertThat(KafkaTrackingToken.empty().position()).hasValue(0L);
        }

        @Test
        void partitionAtZeroEqualsUnmentionedPartition() {
            // when / then
            assertThat(KafkaTrackingToken.of(Map.of(P0, 0L))).isEqualTo(KafkaTrackingToken.empty())
                                                           .hasSameHashCodeAs(KafkaTrackingToken.empty());
        }
    }

    @Nested
    class Creation {

        @Test
        void rejectsAStoredKeyThatIsNoTopicPartition() {
            // when / then
            assertThatThrownBy(() -> new KafkaTrackingToken(Map.of("parcels", 3L)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("[parcels]");
        }

        @Test
        void rejectsNegativeOffsets() {
            // when / then
            assertThatThrownBy(() -> KafkaTrackingToken.of(Map.of(P0, -1L)))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void survivesJsonRoundTripIncludingDashedTopicNames() {
            // given
            JacksonConverter converter = new JacksonConverter();
            KafkaTrackingToken token = KafkaTrackingToken.of(Map.of(P0, 5L, DASHED, 42L));

            // when
            byte[] json = converter.convert(token, byte[].class);
            KafkaTrackingToken result = converter.convert(json, KafkaTrackingToken.class);

            // then
            assertThat(new String(json)).contains("\"parcel-events-10\":42")
                                        .doesNotContain("serializedPositions");
            assertThat(result).isEqualTo(token);
            assertThat(result.positions()).isEqualTo(token.positions());
        }
    }

    @Nested
    class ReplayTokens {

        @Test
        void worksInsideReplayToken() {
            // given
            KafkaTrackingToken head = KafkaTrackingToken.of(Map.of(P0, 10L));
            KafkaTrackingToken start = KafkaTrackingToken.empty();

            // when
            TrackingToken replay = ReplayToken.createReplayToken(head, start);
            TrackingToken stillReplaying = ((ReplayToken) replay).advancedTo(KafkaTrackingToken.of(Map.of(P0, 10L)));
            TrackingToken pastReset = ((ReplayToken) replay).advancedTo(KafkaTrackingToken.of(Map.of(P0, 11L)));

            // then
            assertThat(ReplayToken.isReplay(stillReplaying)).isTrue();
            assertThat(pastReset).isEqualTo(KafkaTrackingToken.of(Map.of(P0, 11L)));
        }
    }
}
