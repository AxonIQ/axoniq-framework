/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jpa;

import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that selecting {@link WorkflowIndexEntry entries} by the range of segment keys a segment owns selects
 * exactly the workflow ids {@link Segment#matches(Object)} assigns to that segment.
 *
 * @author Allard Buijze
 */
class WorkflowIndexEntrySegmentKeyTest {

    private static final List<String> IDS = Stream.concat(
            IntStream.range(0, 2_000).mapToObj(i -> "workflow-" + i),
            IntStream.range(0, 2_000).mapToObj(i -> UUID.randomUUID().toString())
    ).toList();

    /**
     * The ids the given range selects differently than the given segment matches them.
     */
    private static List<String> mismatches(WorkflowIndexEntry.SegmentKeyRange range, Segment segment) {
        return IDS.stream().filter(id -> {
            long key = WorkflowIndexEntry.segmentKey(id);
            boolean inRange = key >= range.lowest() && key <= range.highest();
            return inRange != segment.matches(id);
        }).toList();
    }

    @Nested
    class RangeOf {

        @Test
        void selectsExactlyTheIdsEachSegmentMatchesForEveryContiguousMask() {
            // given - ids with both positive and negative hashes
            assertThat(IDS).anyMatch(id -> id.hashCode() < 0).anyMatch(id -> id.hashCode() >= 0);

            for (int bits = 0; bits <= 8; bits++) {
                int mask = (1 << bits) - 1;
                for (int segmentId = 0; segmentId <= mask; segmentId++) {
                    var segment = new Segment(segmentId, mask);

                    // when
                    var range = WorkflowIndexEntry.rangeOf(segment);

                    // then
                    assertThat(mismatches(range, segment)).as("ids selected differently for %s", segment).isEmpty();
                }
            }
        }

        @Test
        void selectsExactlyTheIdsOfSegmentsWithTheWidestMasks() {
            // given - the widest masks, where segment ids reach into the sign bit
            var segments = List.of(new Segment(0x7FFFFFFF, 0x7FFFFFFF),
                                   new Segment(0x12345678, 0x7FFFFFFF),
                                   new Segment(-1, -1),
                                   new Segment(IDS.getFirst().hashCode(), -1));

            for (Segment segment : segments) {
                // when
                var range = WorkflowIndexEntry.rangeOf(segment);

                // then
                assertThat(mismatches(range, segment)).as("ids selected differently for %s", segment).isEmpty();
            }
        }

        @Test
        void coversEveryKeyForTheRootSegment() {
            // when
            var range = WorkflowIndexEntry.rangeOf(Segment.ROOT_SEGMENT);

            // then
            assertThat(range).isEqualTo(new WorkflowIndexEntry.SegmentKeyRange(0, 0xFFFFFFFFL));
        }

        @Test
        void isEmptyForASegmentIdOutsideItsMask() {
            // given - a segment that matches no id
            var segment = new Segment(4, 3);

            // when
            var range = WorkflowIndexEntry.rangeOf(segment);

            // then
            assertThat(range.lowest()).isGreaterThan(range.highest());
            assertThat(IDS).noneMatch(segment::matches);
        }
    }

    @Nested
    class SegmentKey {

        @Test
        void isAnUnsignedThirtyTwoBitValue() {
            // when / then
            assertThat(IDS).allSatisfy(id -> assertThat(WorkflowIndexEntry.segmentKey(id))
                    .isBetween(0L, 0xFFFFFFFFL));
        }
    }
}
