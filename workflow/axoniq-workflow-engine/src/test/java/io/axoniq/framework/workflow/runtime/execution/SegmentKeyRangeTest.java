/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a {@link SegmentKeyRange} selects exactly the workflow ids its {@link Segment} owns.
 *
 * @author Stefan Dragisic
 */
class SegmentKeyRangeTest {

    private static List<String> workflowIds() {
        // A fixed seed, so a failure names ids that fail again.
        var random = new Random(42);
        return Stream.generate(() -> "workflow-" + random.nextLong()).limit(2_000).toList();
    }

    @Nested
    class KeysOfASegment {

        @Test
        void theRootSegmentOwnsEveryKey() {
            // given
            var range = SegmentKeyRange.of(Segment.ROOT_SEGMENT);

            // when / then
            assertThat(range).isEqualTo(new SegmentKeyRange(0L, 0xFFFF_FFFFL));
            assertThat(workflowIds()).allSatisfy(
                    workflowId -> assertThat(range.contains(SegmentKeyRange.keyOf(workflowId))).isTrue()
            );
        }

        @Test
        void aRangeHoldsExactlyTheWorkflowIdsItsSegmentOwns() {
            // given -- 1, 2, 4, 8, 16, 32 and 64 segments
            var workflowIds = workflowIds();

            for (int splits = 0; splits <= 6; splits++) {
                for (Segment segment : Segment.splitBalanced(Segment.ROOT_SEGMENT, (1 << splits) - 1)) {
                    // when
                    var range = SegmentKeyRange.of(segment);

                    // then
                    assertThat(workflowIds).allSatisfy(
                            workflowId -> assertThat(range.contains(SegmentKeyRange.keyOf(workflowId)))
                                    .as("workflow id %s in %s", workflowId, segment)
                                    .isEqualTo(WorkflowSegmentOwnership.ownedBy(segment, workflowId))
                    );
                }
            }
        }

        @Test
        void theRangesOfAllSegmentsCoverEveryKeyOnce() {
            // given
            var segments = Segment.splitBalanced(Segment.ROOT_SEGMENT, 15);

            // when
            var ranges = segments.stream().map(SegmentKeyRange::of).toList();

            // then
            assertThat(workflowIds()).allSatisfy(workflowId -> assertThat(
                    ranges.stream().filter(range -> range.contains(SegmentKeyRange.keyOf(workflowId)))
            ).hasSize(1));
        }
    }

    @Nested
    class SplitAndMerge {

        @Test
        void aSplitHalvesTheRange() {
            // given
            var segment = Segment.splitBalanced(Segment.ROOT_SEGMENT, 3).get(2);
            var range = SegmentKeyRange.of(segment);

            // when
            var halves = Stream.of(segment.split()).map(SegmentKeyRange::of).toList();

            // then
            assertThat(halves.get(0).from()).isEqualTo(range.from());
            assertThat(halves.get(1).from()).isEqualTo(halves.get(0).to() + 1);
            assertThat(halves.get(1).to()).isEqualTo(range.to());
        }

        @Test
        void aMergeJoinsTwoNeighbouringRanges() {
            // given
            var halves = Segment.splitBalanced(Segment.ROOT_SEGMENT, 3).get(1).split();

            // when
            var merged = SegmentKeyRange.of(halves[0].mergedWith(halves[1]));

            // then
            assertThat(merged).isEqualTo(new SegmentKeyRange(SegmentKeyRange.of(halves[0]).from(),
                                                             SegmentKeyRange.of(halves[1]).to()));
        }

        @Test
        void theKeyOfAWorkflowIdDoesNotDependOnTheSegments() {
            // given
            var workflowId = "order-4711";

            // when
            var key = SegmentKeyRange.keyOf(workflowId);

            // then -- the same key selects the workflow under any number of segments
            for (int splits = 0; splits <= 6; splits++) {
                var owners = Segment.splitBalanced(Segment.ROOT_SEGMENT, (1 << splits) - 1)
                                    .stream()
                                    .filter(segment -> SegmentKeyRange.of(segment).contains(key))
                                    .toList();
                assertThat(owners).singleElement()
                                  .satisfies(owner -> assertThat(owner.matches(workflowId)).isTrue());
            }
        }
    }
}
