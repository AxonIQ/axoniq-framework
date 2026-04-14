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

package io.axoniq.framework.messaging.eventstreaming;

import org.axonframework.messaging.eventhandling.processing.streaming.token.GapAwareTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for validating the behavior of {@link ReplayToken} when wrapping complex token types.
 * <p>
 * This test validates replay detection across various nested token scenarios:
 * <ul>
 *   <li>{@code MultiSourceTrackingToken} - multi-source scenarios where each source has independent gap positions</li>
 * </ul>
 *
 * @author Mateusz Nowak
 * @since 4.12.3
 */
class ReplayTokenWrappingComplexTokenTest {

    @Nested
    class MultiSourceTrackingTokenAdvancedTo {

        /**
         * Test with MultiSourceTrackingToken scenario.
         * <p>
         * During reset: - A: Index 6, Gaps [1] - B: Index 4, Gaps [1]
         * <p>
         * During replay the new token: - A: Index 2, Gaps [] - B: Index 1, Gaps []
         * <p>
         * Source B's newToken is at index 1, which IS in the gaps of tokenAtReset for B. This means event 1 for source
         * B was NOT processed before reset - it's a new event. Therefore, this should NOT be marked as a replay.
         */
        @Test
        void whenOneSourceAtGapThenNotReplay() {
            // given
            // MultiSourceTrackingToken at reset with gaps
            Map<String, TrackingToken> tokensAtReset = new HashMap<>();
            tokensAtReset.put("A", GapAwareTrackingToken.newInstance(6, Collections.singleton(1L)));
            tokensAtReset.put("B", GapAwareTrackingToken.newInstance(4, Collections.singleton(1L)));
            MultiSourceTrackingToken multiTokenAtReset = new MultiSourceTrackingToken(tokensAtReset);

            // and
            // Create replay token starting from the beginning
            TrackingToken replayToken = ReplayToken.createReplayToken(multiTokenAtReset, null);
            assertThat(replayToken).isInstanceOf(ReplayToken.class);

            // when
            // advancing during replay with a token where B is at a gap position
            Map<String, TrackingToken> newTokens = new HashMap<>();
            newTokens.put("A",
                          GapAwareTrackingToken.newInstance(2,
                                                            emptySet())); // Index 2 was processed before (not in gaps)
            newTokens.put("B",
                          GapAwareTrackingToken.newInstance(1,
                                                            emptySet())); // Index 1 was NOT processed before (in gaps!)
            MultiSourceTrackingToken newMultiToken = new MultiSourceTrackingToken(newTokens);

            TrackingToken advancedToken = ((ReplayToken) replayToken).advancedTo(newMultiToken);

            // then
            // Should still be in replay MODE (not exited yet), but this specific event is NOT a replay
            // because source B's event at index 1 was never processed before reset
            assertThat(advancedToken)
                    .as("Should still be a ReplayToken since we haven't caught up to reset position")
                    .isInstanceOf(ReplayToken.class);

            assertThat(ReplayToken.isReplay(advancedToken))
                    .as("Should NOT be marked as replay - source B's event at index 1 was a gap (never processed before reset)")
                    .isFalse();
        }

        @Test
        void whenBothSourcesNotAtGapThenReplay() {
            // given
            Map<String, TrackingToken> tokensAtReset = new HashMap<>();
            tokensAtReset.put("A", GapAwareTrackingToken.newInstance(6, Collections.singleton(1L)));
            tokensAtReset.put("B", GapAwareTrackingToken.newInstance(4, Collections.singleton(1L)));
            MultiSourceTrackingToken multiTokenAtReset = new MultiSourceTrackingToken(tokensAtReset);

            // and
            TrackingToken replayToken = ReplayToken.createReplayToken(multiTokenAtReset, null);
            assertThat(replayToken).isInstanceOf(ReplayToken.class);

            // when
            Map<String, TrackingToken> newTokens = new HashMap<>();
            newTokens.put("A",
                          GapAwareTrackingToken.newInstance(2,
                                                            emptySet())); // Index 2 was processed before (not in gaps)
            newTokens.put("B",
                          GapAwareTrackingToken.newInstance(2,
                                                            emptySet())); // Index 2 was processed before (not in gaps)
            MultiSourceTrackingToken newMultiToken = new MultiSourceTrackingToken(newTokens);

            TrackingToken advancedToken = ((ReplayToken) replayToken).advancedTo(newMultiToken);

            // then
            assertThat(advancedToken)
                    .as("Should still be a ReplayToken since we haven't caught up to reset position")
                    .isInstanceOf(ReplayToken.class);
            assertThat(ReplayToken.isReplay(advancedToken))
                    .as("Should be marked as replay - both source positions were processed before reset")
                    .isTrue();
        }

        @Test
        void whenOneSourceNotAGapButBetweenGapsThenReplay() {
            // given
            Map<String, TrackingToken> tokensAtReset = new HashMap<>();
            tokensAtReset.put("localEventStore",
                              GapAwareTrackingToken.newInstance(11079,
                                                                list(10155,
                                                                     10156,
                                                                     10157,
                                                                     10158,
                                                                     10159,
                                                                     10160,
                                                                     10161,
                                                                     10162,
                                                                     10163,
                                                                     10164,
                                                                     10165,
                                                                     10166,
                                                                     10167,
                                                                     10168,
                                                                     10169,
                                                                     10170,
                                                                     10171,
                                                                     10172,
                                                                     10173,
                                                                     10174,
                                                                     10175,
                                                                     10176,
                                                                     10177,
                                                                     10178,
                                                                     10179,
                                                                     10180,
                                                                     10198,
                                                                     10199,
                                                                     10200,
                                                                     10201,
                                                                     10202,
                                                                     10203,
                                                                     10204,
                                                                     10205,
                                                                     10206,
                                                                     10207,
                                                                     10208,
                                                                     10209,
                                                                     10210,
                                                                     10211,
                                                                     10212,
                                                                     10213,
                                                                     10214,
                                                                     10215,
                                                                     10216,
                                                                     10217,
                                                                     10218,
                                                                     10219,
                                                                     10220,
                                                                     10221,
                                                                     10222,
                                                                     10223,
                                                                     10224,
                                                                     10225,
                                                                     10226,
                                                                     10227,
                                                                     10228)));
            tokensAtReset.put("globalEventStore",
                              GapAwareTrackingToken.newInstance(38341,
                                                                list(36941,
                                                                     36942,
                                                                     36943,
                                                                     36944,
                                                                     36945,
                                                                     36946,
                                                                     36947,
                                                                     36948,
                                                                     36949,
                                                                     36950,
                                                                     36951,
                                                                     36952,
                                                                     36953,
                                                                     36954,
                                                                     36955,
                                                                     36956,
                                                                     36957,
                                                                     36958,
                                                                     36959,
                                                                     36960,
                                                                     36961,
                                                                     36962,
                                                                     36963,
                                                                     36964,
                                                                     36965,
                                                                     36966,
                                                                     36967,
                                                                     36968,
                                                                     36969,
                                                                     36970,
                                                                     37157,
                                                                     37158,
                                                                     37159,
                                                                     37160,
                                                                     37161,
                                                                     37162,
                                                                     37163,
                                                                     37164,
                                                                     37165,
                                                                     37166,
                                                                     37167,
                                                                     37168,
                                                                     37169,
                                                                     37170,
                                                                     37171,
                                                                     37172,
                                                                     37173,
                                                                     37174,
                                                                     37175,
                                                                     37176,
                                                                     37177,
                                                                     37178,
                                                                     37179,
                                                                     37180,
                                                                     37181,
                                                                     37182,
                                                                     37183,
                                                                     37184,
                                                                     37295,
                                                                     37296,
                                                                     37297,
                                                                     37298,
                                                                     37299,
                                                                     37300,
                                                                     37301,
                                                                     37302,
                                                                     37303,
                                                                     37304,
                                                                     37305,
                                                                     37306,
                                                                     37307,
                                                                     37308,
                                                                     37309,
                                                                     37310,
                                                                     37311,
                                                                     37312,
                                                                     37313,
                                                                     37314,
                                                                     37315,
                                                                     37316,
                                                                     37317,
                                                                     37318,
                                                                     37319,
                                                                     37320,
                                                                     37321,
                                                                     37322,
                                                                     37323,
                                                                     37324,
                                                                     37331,
                                                                     37332,
                                                                     37333,
                                                                     37334,
                                                                     37335,
                                                                     37336,
                                                                     37337,
                                                                     37338,
                                                                     37339,
                                                                     37340,
                                                                     37341,
                                                                     37342,
                                                                     37343,
                                                                     37344,
                                                                     37345,
                                                                     37346,
                                                                     37347,
                                                                     37348,
                                                                     37349,
                                                                     37350,
                                                                     37351,
                                                                     37352,
                                                                     37353,
                                                                     37354,
                                                                     37355,
                                                                     37356,
                                                                     37357,
                                                                     37358,
                                                                     37359,
                                                                     37360,
                                                                     37361,
                                                                     37362,
                                                                     37717,
                                                                     37718,
                                                                     37719,
                                                                     37720,
                                                                     37721,
                                                                     37722,
                                                                     37723,
                                                                     37724,
                                                                     37725,
                                                                     37726,
                                                                     37727,
                                                                     37728,
                                                                     37729,
                                                                     37730,
                                                                     37731,
                                                                     37732,
                                                                     37733,
                                                                     37734,
                                                                     37897,
                                                                     37898,
                                                                     37899,
                                                                     37900,
                                                                     37901,
                                                                     37902,
                                                                     37903,
                                                                     37904,
                                                                     37905,
                                                                     37906,
                                                                     37907,
                                                                     37908,
                                                                     37909,
                                                                     37910,
                                                                     37911,
                                                                     37912,
                                                                     37913,
                                                                     37914,
                                                                     37915,
                                                                     37916,
                                                                     37917,
                                                                     37918,
                                                                     37919,
                                                                     37920,
                                                                     37921,
                                                                     37922,
                                                                     37923,
                                                                     37924,
                                                                     37925,
                                                                     37926,
                                                                     37927)));
            MultiSourceTrackingToken multiTokenAtReset = new MultiSourceTrackingToken(tokensAtReset);

            // and
            Map<String, TrackingToken> startPosition = new HashMap<>();
            startPosition.put("localEventStore", GapAwareTrackingToken.newInstance(9960, emptySet()));
            startPosition.put("globalEventStore", GapAwareTrackingToken.newInstance(37192, emptySet()));
            MultiSourceTrackingToken multiStartPosition = new MultiSourceTrackingToken(startPosition);

            TrackingToken replayToken = ReplayToken.createReplayToken(multiTokenAtReset, multiStartPosition);

            // when
            Map<String, TrackingToken> nextPositions = new HashMap<>();
            nextPositions.put("localEventStore", GapAwareTrackingToken.newInstance(9961, emptySet()));
            nextPositions.put("globalEventStore", GapAwareTrackingToken.newInstance(37192, emptySet()));
            TrackingToken nextToken = new MultiSourceTrackingToken(nextPositions);

            TrackingToken advancedToken = ((ReplayToken) replayToken).advancedTo(nextToken);

            // then
            assertThat(advancedToken).isInstanceOf(ReplayToken.class);
            assertThat(ReplayToken.isReplay(advancedToken)).isTrue();
        }

        private List<Long> list(Integer... values) {
            return Stream.of(values)
                         .map(Long::valueOf)
                         .toList();
        }
    }
}
