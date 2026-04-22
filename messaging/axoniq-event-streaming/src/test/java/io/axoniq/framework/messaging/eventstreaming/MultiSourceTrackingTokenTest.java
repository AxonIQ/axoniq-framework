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

import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link MultiSourceTrackingToken}.
 *
 * @author Greg Woods
 */
class MultiSourceTrackingTokenTest {

    private MultiSourceTrackingToken testSubject;

    @BeforeEach
    void setUp() {
        Map<String, TrackingToken> tokenMap = new HashMap<>();
        tokenMap.put("token1", new GlobalSequenceTrackingToken(0));
        tokenMap.put("token2", new GlobalSequenceTrackingToken(0));

        testSubject = new MultiSourceTrackingToken(tokenMap);
    }

    @Test
    void incompatibleToken() {
        assertThatThrownBy(() -> testSubject.covers(new GlobalSequenceTrackingToken(0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void trackingTokenIsImmutable() {
        MultiSourceTrackingToken newToken = testSubject.advancedTo("token1", new GlobalSequenceTrackingToken(1));

        assertThat(testSubject.getTokenForStream("token1")).isEqualTo(new GlobalSequenceTrackingToken(0));
        assertThat(testSubject.getTokenForStream("token2")).isEqualTo(new GlobalSequenceTrackingToken(0));
        assertThat(newToken.getTokenForStream("token1")).isEqualTo(new GlobalSequenceTrackingToken(1));
        assertThat(newToken.getTokenForStream("token2")).isEqualTo(new GlobalSequenceTrackingToken(0));
    }

    @Test
    void lowerBound() {
        Map<String, TrackingToken> newTokens = new HashMap<>();
        newTokens.put("token1", new GlobalSequenceTrackingToken(1));
        newTokens.put("token2", new GlobalSequenceTrackingToken(2));

        Map<String, TrackingToken> expectedTokens = new HashMap<>();
        expectedTokens.put("token1", new GlobalSequenceTrackingToken(0));
        expectedTokens.put("token2", new GlobalSequenceTrackingToken(0));

        MultiSourceTrackingToken newMultiToken =
                (MultiSourceTrackingToken) testSubject.lowerBound(new MultiSourceTrackingToken(newTokens));

        assertThat(newMultiToken).isEqualTo(new MultiSourceTrackingToken(expectedTokens));
    }

    @Test
    void lowerBoundWithNullOnThisToken() {
        Map<String, TrackingToken> tokens = new HashMap<>();
        tokens.put("token1", new GlobalSequenceTrackingToken(2));
        tokens.put("token2", null);

        testSubject = new MultiSourceTrackingToken(tokens);

        Map<String, TrackingToken> otherTokens = new HashMap<>();
        otherTokens.put("token1", new GlobalSequenceTrackingToken(1));
        otherTokens.put("token2", new GlobalSequenceTrackingToken(2));

        Map<String, TrackingToken> expectedTokens = new HashMap<>();
        expectedTokens.put("token1", new GlobalSequenceTrackingToken(1));
        expectedTokens.put("token2", null);

        MultiSourceTrackingToken result =
                (MultiSourceTrackingToken) testSubject.lowerBound(new MultiSourceTrackingToken(otherTokens));

        assertThat(result).isEqualTo(new MultiSourceTrackingToken(expectedTokens));
    }

    @Test
    void lowerBoundWithNullOnOtherToken() {
        Map<String, TrackingToken> tokens = new HashMap<>();
        tokens.put("token1", new GlobalSequenceTrackingToken(2));
        tokens.put("token2", new GlobalSequenceTrackingToken(2));

        testSubject = new MultiSourceTrackingToken(tokens);

        Map<String, TrackingToken> otherTokens = new HashMap<>();
        otherTokens.put("token1", new GlobalSequenceTrackingToken(1));
        otherTokens.put("token2", null);

        Map<String, TrackingToken> expectedTokens = new HashMap<>();
        expectedTokens.put("token1", new GlobalSequenceTrackingToken(1));
        expectedTokens.put("token2", null);

        MultiSourceTrackingToken result =
                (MultiSourceTrackingToken) testSubject.lowerBound(new MultiSourceTrackingToken(otherTokens));

        assertThat(result).isEqualTo(new MultiSourceTrackingToken(expectedTokens));
    }

    @Test
    void lowerBoundMismatchTokens() {
        Map<String, TrackingToken> newTokens = new HashMap<>();
        newTokens.put("token1", new GlobalSequenceTrackingToken(1));
        newTokens.put("token3", new GlobalSequenceTrackingToken(2));

        assertThatThrownBy(() -> testSubject.lowerBound(new MultiSourceTrackingToken(newTokens)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lowerBoundDifferentNumberOfTokens() {
        Map<String, TrackingToken> newTokens = new HashMap<>();
        newTokens.put("token1", new GlobalSequenceTrackingToken(1));

        assertThatThrownBy(() -> testSubject.lowerBound(new MultiSourceTrackingToken(newTokens)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void positionNotProvidedWhenUnderlyingTokensDontProvide() {
        TrackingToken trackingToken = mock(TrackingToken.class);
        when(trackingToken.position()).thenReturn(OptionalLong.empty());
        testSubject = new MultiSourceTrackingToken(Collections.singletonMap("key", trackingToken));

        assertThat(testSubject.position()).isEmpty();
    }

    @Test
    void upperBound() {
        Map<String, TrackingToken> newTokens = new HashMap<>();

        newTokens.put("token1", new GlobalSequenceTrackingToken(1));
        newTokens.put("token2", new GlobalSequenceTrackingToken(2));

        MultiSourceTrackingToken newMultiToken = testSubject.upperBound(new MultiSourceTrackingToken(newTokens));

        assertThat(newMultiToken).isEqualTo(new MultiSourceTrackingToken(newTokens));
    }

    @Test
    void upperBoundWithNullInThisToken() {
        Map<String, TrackingToken> tokens = new HashMap<>();
        tokens.put("token1", new GlobalSequenceTrackingToken(0));
        tokens.put("token2", null);

        testSubject = new MultiSourceTrackingToken(tokens);
        Map<String, TrackingToken> otherTokens = new HashMap<>();

        otherTokens.put("token1", new GlobalSequenceTrackingToken(1));
        otherTokens.put("token2", new GlobalSequenceTrackingToken(2));

        MultiSourceTrackingToken result = testSubject.upperBound(new MultiSourceTrackingToken(otherTokens));

        assertThat(result).isEqualTo(new MultiSourceTrackingToken(otherTokens));
    }

    @Test
    void upperBoundWithNullInOtherToken() {
        Map<String, TrackingToken> expectedTokens = new HashMap<>();
        expectedTokens.put("token1", new GlobalSequenceTrackingToken(1));
        expectedTokens.put("token2", new GlobalSequenceTrackingToken(2));


        Map<String, TrackingToken> tokens = new HashMap<>();
        tokens.put("token1", new GlobalSequenceTrackingToken(0));
        tokens.put("token2", new GlobalSequenceTrackingToken(2));

        testSubject = new MultiSourceTrackingToken(tokens);
        Map<String, TrackingToken> otherTokens = new HashMap<>();

        otherTokens.put("token1", new GlobalSequenceTrackingToken(1));
        otherTokens.put("token2", null);

        MultiSourceTrackingToken result = testSubject.upperBound(new MultiSourceTrackingToken(otherTokens));

        assertThat(result).isEqualTo(new MultiSourceTrackingToken(expectedTokens));
    }

    @Test
    void upperBoundMismatchTokens() {
        Map<String, TrackingToken> newTokens = new HashMap<>();
        newTokens.put("token1", new GlobalSequenceTrackingToken(1));
        newTokens.put("token3", new GlobalSequenceTrackingToken(2));

        assertThatThrownBy(() -> testSubject.upperBound(new MultiSourceTrackingToken(newTokens)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void upperBoundDifferentNumberOfTokens() {
        Map<String, TrackingToken> newTokens = new HashMap<>();
        newTokens.put("token1", new GlobalSequenceTrackingToken(1));

        assertThatThrownBy(() -> testSubject.upperBound(new MultiSourceTrackingToken(newTokens)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void covers() {
        Map<String, TrackingToken> newTokens = new HashMap<>();
        newTokens.put("token1", new GlobalSequenceTrackingToken(0));
        newTokens.put("token2", new GlobalSequenceTrackingToken(0));

        assertThat(testSubject.covers(new MultiSourceTrackingToken(newTokens))).isTrue();
    }

    @Test
    void doesNotCover() {
        Map<String, TrackingToken> newTokens = new HashMap<>();
        newTokens.put("token1", new GlobalSequenceTrackingToken(1));
        newTokens.put("token2", new GlobalSequenceTrackingToken(0));

        assertThat(testSubject.covers(new MultiSourceTrackingToken(newTokens))).isFalse();
    }

    @Test
    void coversNullConstituents() {
        Map<String, TrackingToken> newTokens = new HashMap<>();
        newTokens.put("token1", new GlobalSequenceTrackingToken(0));
        newTokens.put("token2", null);
        MultiSourceTrackingToken tokenWithNullConstituent = new MultiSourceTrackingToken(newTokens);

        assertThat(tokenWithNullConstituent.covers(tokenWithNullConstituent)).isTrue();
        assertThat(testSubject.covers(tokenWithNullConstituent)).isTrue();
        assertThat(tokenWithNullConstituent.covers(testSubject)).isFalse();
    }


    @Test
    void coversMismatchTokens() {
        Map<String, TrackingToken> newTokens = new HashMap<>();
        newTokens.put("token1", new GlobalSequenceTrackingToken(1));
        newTokens.put("token3", new GlobalSequenceTrackingToken(2));

        assertThatThrownBy(() -> testSubject.covers(new MultiSourceTrackingToken(newTokens)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void coversDifferentNumberOfTokens() {
        Map<String, TrackingToken> newTokens = new HashMap<>();
        newTokens.put("token1", new GlobalSequenceTrackingToken(1));

        assertThatThrownBy(() -> testSubject.covers(new MultiSourceTrackingToken(newTokens)))
                .isInstanceOf(IllegalArgumentException.class);
    }


    @Test
    void advancedTo() {
        MultiSourceTrackingToken result = testSubject.advancedTo("token1", new GlobalSequenceTrackingToken(4));

        assertThat(result.getTokenForStream("token1")).isEqualTo(new GlobalSequenceTrackingToken(4));
        //other token remains unchanged
        assertThat(result.getTokenForStream("token2")).isEqualTo(new GlobalSequenceTrackingToken(0));
    }

    @Test
    void getTokenForStream() {
        assertThat(testSubject.getTokenForStream("token1"))
                .isEqualTo(new GlobalSequenceTrackingToken(0));
    }

    @Test
    void hasPosition() {
        assertThat(testSubject.position()).isPresent();
        assertThat(testSubject.position().getAsLong()).isZero();
    }

    @Test
    void positionWithNullValue() {
        Map<String, TrackingToken> tokenMap = new HashMap<>();
        tokenMap.put("token1", new GlobalSequenceTrackingToken(10));
        tokenMap.put("token2", null);

        MultiSourceTrackingToken newMultiToken = new MultiSourceTrackingToken(tokenMap);

        assertThat(newMultiToken.position().orElse(-1)).isEqualTo(10);
    }

    @Test
    void positionWithNullValue2() {
        Map<String, TrackingToken> tokenMap = new HashMap<>();
        tokenMap.put("token1", null);
        tokenMap.put("token2", new GlobalSequenceTrackingToken(10));

        MultiSourceTrackingToken newMultiToken = new MultiSourceTrackingToken(tokenMap);

        assertThat(newMultiToken.position().orElse(-1)).isEqualTo(10);
    }

    @Test
    void equals() {
        Map<String, TrackingToken> tokenMap = new HashMap<>();
        tokenMap.put("token1", new GlobalSequenceTrackingToken(0));
        tokenMap.put("token2", new GlobalSequenceTrackingToken(0));

        MultiSourceTrackingToken newMultiToken = new MultiSourceTrackingToken(tokenMap);

        assertThat(testSubject).isEqualTo(newMultiToken);
    }

    @Test
    void notEquals() {
        Map<String, TrackingToken> tokenMap = new HashMap<>();
        tokenMap.put("token1", new GlobalSequenceTrackingToken(1));
        tokenMap.put("token2", new GlobalSequenceTrackingToken(0));

        MultiSourceTrackingToken newMultiToken = new MultiSourceTrackingToken(tokenMap);

        assertThat(testSubject).isNotEqualTo(newMultiToken);
    }

    @Test
    void constituentTokenNotInitialized() {
        Map<String, TrackingToken> tokenMap = new HashMap<>();
        tokenMap.put("token1", null);
        tokenMap.put("token2", new GlobalSequenceTrackingToken(0));

        MultiSourceTrackingToken newMultiToken = new MultiSourceTrackingToken(tokenMap);

        assertThat(testSubject).isNotEqualTo(newMultiToken);
    }

    @Test
    void samePositionAs() {
        Map<String, TrackingToken> samePositionTokens = new HashMap<>();
        samePositionTokens.put("token1", new GlobalSequenceTrackingToken(0));
        samePositionTokens.put("token2", new GlobalSequenceTrackingToken(0));

        assertThat(testSubject.samePositionAs(new MultiSourceTrackingToken(samePositionTokens))).isTrue();
    }

    @Test
    void samePositionAsReturnsFalseWhenConstituentsAreDifferent() {
        Map<String, TrackingToken> differentTokens = new HashMap<>();
        differentTokens.put("token1", new GlobalSequenceTrackingToken(1));
        differentTokens.put("token2", new GlobalSequenceTrackingToken(0));

        assertThat(testSubject.samePositionAs(new MultiSourceTrackingToken(differentTokens))).isFalse();
    }

    @Test
    void samePositionAsWithNullConstituents() {
        Map<String, TrackingToken> tokensWithNull = new HashMap<>();
        tokensWithNull.put("token1", new GlobalSequenceTrackingToken(0));
        tokensWithNull.put("token2", null);
        MultiSourceTrackingToken tokenWithNull = new MultiSourceTrackingToken(tokensWithNull);

        // Comparing two tokens with samePositionAs null constituent
        assertThat(tokenWithNull.samePositionAs(tokenWithNull)).isTrue();

        // Comparing token with null constituent to token without
        assertThat(tokenWithNull.samePositionAs(testSubject)).isFalse();
        assertThat(testSubject.samePositionAs(tokenWithNull)).isFalse();
    }

    @Test
    void samePositionAsMismatchTokens() {
        Map<String, TrackingToken> differentKeys = new HashMap<>();
        differentKeys.put("token1", new GlobalSequenceTrackingToken(0));
        differentKeys.put("token3", new GlobalSequenceTrackingToken(0));

        assertThatThrownBy(() -> testSubject.samePositionAs(new MultiSourceTrackingToken(differentKeys)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void samePositionAsDifferentNumberOfTokens() {
        Map<String, TrackingToken> fewerTokens = new HashMap<>();
        fewerTokens.put("token1", new GlobalSequenceTrackingToken(0));

        assertThatThrownBy(() -> testSubject.samePositionAs(new MultiSourceTrackingToken(fewerTokens)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void samePositionAsIncompatibleToken() {
        assertThatThrownBy(() -> testSubject.samePositionAs(new GlobalSequenceTrackingToken(0)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
