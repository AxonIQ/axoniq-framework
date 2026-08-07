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

package io.axoniq.framework.messaging.multitenancy.eventstreaming;

import org.axonframework.conversion.TestConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the tolerant, changing-tenant-set semantics of {@link MultiTenantTrackingToken}.
 *
 * @author Laura Devriendt
 */
class MultiTenantTrackingTokenTest {

    private final MultiTenantTrackingToken testSubject = new MultiTenantTrackingToken(Map.of(
            "tenant1", new GlobalSequenceTrackingToken(0),
            "tenant2", new GlobalSequenceTrackingToken(0)
    ));

    @Nested
    class Adapting {

        @Test
        void fromReturnsTheSameInstanceForAMultiTenantToken() {
            assertThat(MultiTenantTrackingToken.from(testSubject)).isSameAs(testSubject);
        }

        @Test
        void fromNullReturnsAnEmptyToken() {
            assertThat(MultiTenantTrackingToken.from(null)).isEqualTo(MultiTenantTrackingToken.empty());
        }

        @Test
        void fromRejectsAnIncompatibleTokenType() {
            GlobalSequenceTrackingToken incompatible = new GlobalSequenceTrackingToken(0);

            assertThatThrownBy(() -> MultiTenantTrackingToken.from(incompatible))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class Positioning {

        @Test
        void advancedToReplacesTheTenantPosition() {
            MultiTenantTrackingToken advanced = testSubject.advancedTo("tenant1", new GlobalSequenceTrackingToken(5));

            assertThat(advanced.tokenForTenant("tenant1")).isEqualTo(new GlobalSequenceTrackingToken(5));
            assertThat(advanced.tokenForTenant("tenant2")).isEqualTo(new GlobalSequenceTrackingToken(0));
        }

        @Test
        void advancedToAddsAPreviouslyAbsentTenant() {
            MultiTenantTrackingToken advanced = testSubject.advancedTo("tenant3", new GlobalSequenceTrackingToken(2));

            assertThat(advanced.tokenForTenant("tenant3")).isEqualTo(new GlobalSequenceTrackingToken(2));
        }

        @Test
        void tokenForTenantIsNullWhenAbsent() {
            assertThat(testSubject.tokenForTenant("unknown")).isNull();
        }
    }

    @Nested
    class Comparing {

        @Test
        void coversToleratesAnAdvancedTenantOnlyTheOtherTracks() {
            MultiTenantTrackingToken other = new MultiTenantTrackingToken(Map.of(
                    "tenant1", new GlobalSequenceTrackingToken(0),
                    "tenant3", new GlobalSequenceTrackingToken(2)
            ));

            // tenant3 is tracked only by other and has advanced, so this does not cover it, and no exception is thrown
            assertThat(testSubject.covers(other)).isFalse();
        }

        @Test
        void coversWhenThisTracksMoreTenantsThanOther() {
            MultiTenantTrackingToken other =
                    new MultiTenantTrackingToken(Map.of("tenant1", new GlobalSequenceTrackingToken(0)));

            // tenant2 is tracked only by this token, which imposes no constraint on covering other
            assertThat(testSubject.covers(other)).isTrue();
        }

        @Test
        void lowerBoundOverTheUnionOfTenants() {
            MultiTenantTrackingToken other = new MultiTenantTrackingToken(Map.of(
                    "tenant1", new GlobalSequenceTrackingToken(1),
                    "tenant3", new GlobalSequenceTrackingToken(2)
            ));

            Map<String, @Nullable TrackingToken> expected = new HashMap<>();
            expected.put("tenant1", new GlobalSequenceTrackingToken(0));
            expected.put("tenant2", null);
            expected.put("tenant3", null);

            assertThat(testSubject.lowerBound(other)).isEqualTo(new MultiTenantTrackingToken(expected));
        }

        @Test
        void upperBoundOverTheUnionOfTenants() {
            MultiTenantTrackingToken other = new MultiTenantTrackingToken(Map.of(
                    "tenant1", new GlobalSequenceTrackingToken(1),
                    "tenant3", new GlobalSequenceTrackingToken(2)
            ));

            Map<String, @Nullable TrackingToken> expected = new HashMap<>();
            expected.put("tenant1", new GlobalSequenceTrackingToken(1));
            expected.put("tenant2", new GlobalSequenceTrackingToken(0));
            expected.put("tenant3", new GlobalSequenceTrackingToken(2));

            assertThat(testSubject.upperBound(other)).isEqualTo(new MultiTenantTrackingToken(expected));
        }

        @Test
        void notSamePositionWhenOtherTracksADifferentTenant() {
            MultiTenantTrackingToken other = new MultiTenantTrackingToken(Map.of(
                    "tenant1", new GlobalSequenceTrackingToken(0),
                    "tenant3", new GlobalSequenceTrackingToken(0)
            ));

            // tenant2 is tracked only by this token, so the positions differ, and no exception is thrown
            assertThat(testSubject.samePositionAs(other)).isFalse();
        }

        @Test
        void samePositionWhenEveryTenantMatches() {
            MultiTenantTrackingToken other = new MultiTenantTrackingToken(Map.of(
                    "tenant1", new GlobalSequenceTrackingToken(0),
                    "tenant2", new GlobalSequenceTrackingToken(0)
            ));

            assertThat(testSubject.samePositionAs(other)).isTrue();
        }

        @Test
        void notSamePositionWhenThisIsMissingATenantTheOtherPositions() {
            MultiTenantTrackingToken other = new MultiTenantTrackingToken(Map.of(
                    "tenant1", new GlobalSequenceTrackingToken(0),
                    "tenant2", new GlobalSequenceTrackingToken(0),
                    "tenant3", new GlobalSequenceTrackingToken(0)
            ));

            // tenant3 is tracked only by other, so this is behind it
            assertThat(testSubject.samePositionAs(other)).isFalse();
        }

        @Test
        void comparingRejectsAnIncompatibleTokenType() {
            GlobalSequenceTrackingToken incompatible = new GlobalSequenceTrackingToken(0);

            assertThatThrownBy(() -> testSubject.covers(incompatible))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class Position {

        @Test
        void sumsTheTenantPositions() {
            MultiTenantTrackingToken token = new MultiTenantTrackingToken(Map.of(
                    "tenant1", new GlobalSequenceTrackingToken(3),
                    "tenant2", new GlobalSequenceTrackingToken(4)
            ));

            assertThat(token.position()).hasValue(7L);
        }

        @Test
        void isEmptyWhenNoTenantIsPositioned() {
            Map<String, @Nullable TrackingToken> tokenMap = new HashMap<>();
            tokenMap.put("tenant1", null);
            MultiTenantTrackingToken token = new MultiTenantTrackingToken(tokenMap);

            assertThat(token.position()).isEqualTo(OptionalLong.empty());
        }

        @Test
        void isEmptyForAnEmptyToken() {
            assertThat(MultiTenantTrackingToken.empty().position()).isEqualTo(OptionalLong.empty());
        }
    }

    @Nested
    class Equality {

        @Test
        void equalWhenTheTenantPositionsMatch() {
            MultiTenantTrackingToken other = new MultiTenantTrackingToken(Map.of(
                    "tenant1", new GlobalSequenceTrackingToken(0),
                    "tenant2", new GlobalSequenceTrackingToken(0)
            ));

            assertThat(testSubject).isEqualTo(other).hasSameHashCodeAs(other);
        }

        @Test
        void notEqualWhenTheTenantPositionsDiffer() {
            MultiTenantTrackingToken other = new MultiTenantTrackingToken(Map.of(
                    "tenant1", new GlobalSequenceTrackingToken(1),
                    "tenant2", new GlobalSequenceTrackingToken(0)
            ));

            assertThat(testSubject).isNotEqualTo(other).isNotEqualTo(MultiTenantTrackingToken.empty());
        }

        @Test
        void toStringListsTheTenantPositions() {
            assertThat(testSubject.toString())
                    .startsWith("MultiTenantTrackingToken{")
                    .contains("tenant1=")
                    .contains("tenant2=");
        }
    }

    @Nested
    class Serializing {

        @MethodSource("converters")
        @ParameterizedTest
        void tokenIsSerializable(TestConverter converter) {
            Map<String, @Nullable TrackingToken> tokenMap = new HashMap<>();
            tokenMap.put("tenant1", new GlobalSequenceTrackingToken(1));
            tokenMap.put("tenant2", null);
            MultiTenantTrackingToken original = new MultiTenantTrackingToken(tokenMap);

            assertThat(converter.serializeDeserialize(original)).isEqualTo(original);
        }

        static Collection<TestConverter> converters() {
            return TestConverter.all();
        }
    }
}
