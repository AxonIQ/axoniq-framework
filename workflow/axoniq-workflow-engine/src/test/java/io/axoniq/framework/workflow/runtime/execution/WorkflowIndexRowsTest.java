/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges.SegmentPosition;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests which position the workflow manager reads from, given the positions stored for the ranges that overlap a
 * segment.
 *
 * @author Stefan Dragisic
 */
class WorkflowIndexRowsTest {

    private static final Segment ROOT = Segment.ROOT_SEGMENT;
    private static final Segment[] HALVES = ROOT.split();
    private static final Segment[] QUARTERS_OF_LOWER_HALF = HALVES[0].split();

    @Nested
    class PositionOf {

        @Test
        void isThePositionOfTheSegmentItself() {
            // given
            var rows = rows(position(HALVES[0], 10L));

            // when
            var position = rows.positionOf(HALVES[0]);

            // then
            assertThat(position).hasValue(10L);
        }

        @Test
        void isThePositionOfTheSegmentASplitReplaced() {
            // given
            var rows = rows(position(ROOT, 10L));

            // when
            var position = rows.positionOf(HALVES[1]);

            // then
            assertThat(position).hasValue(10L);
        }

        @Test
        void isTheLowerPositionOfTheSegmentsAMergeReplaced() {
            // given
            var rows = rows(position(HALVES[0], 20L), position(HALVES[1], 15L));

            // when
            var position = rows.positionOf(ROOT);

            // then
            assertThat(position).hasValue(15L);
        }

        @Test
        void isTheLowestPositionOfTheSegmentsSeveralSplitsAndAMergeReplaced() {
            // given
            var rows = rows(position(QUARTERS_OF_LOWER_HALF[0], 30L),
                            position(QUARTERS_OF_LOWER_HALF[1], 31L),
                            position(HALVES[1], 25L));

            // when
            var position = rows.positionOf(ROOT);

            // then
            assertThat(position).hasValue(25L);
        }

        @Test
        void prefersTheHigherPositionsOfTheHalvesOverTheOldPositionOfTheSegmentHoldingThem() {
            // given: a position the segment stored before it split, and the positions both halves stored since
            var rows = rows(position(ROOT, 5L), position(HALVES[0], 20L), position(HALVES[1], 15L));

            // when
            var position = rows.positionOf(ROOT);

            // then
            assertThat(position).hasValue(15L);
        }

        @Test
        void isTheOldPositionOfTheSegmentHoldingAHalfWithoutAPositionOfItsOwn() {
            // given: the old segment, and only one half with a position of its own
            var rows = rows(position(ROOT, 5L), position(HALVES[0], 20L));

            // when
            var position = rows.positionOf(ROOT);

            // then: the keys of the other half are only held by the old position
            assertThat(position).hasValue(5L);
        }

        @Test
        void isEmptyWithoutPositions() {
            // given
            var rows = rows();

            // when
            var position = rows.positionOf(ROOT);

            // then
            assertThat(position).isEmpty();
        }

        @Test
        void isEmptyWhenAPartOfTheSegmentHasNoPosition() {
            // given: no position for the lowest quarter
            var rows = rows(position(QUARTERS_OF_LOWER_HALF[1], 20L), position(HALVES[1], 21L));

            // when
            var position = rows.positionOf(ROOT);

            // then
            assertThat(position).isEmpty();
        }
    }

    private static WorkflowIndexRows rows(SegmentPosition... positions) {
        return new WorkflowIndexRows(Set.of(), Map.of(), List.of(), List.of(positions));
    }

    private static SegmentPosition position(Segment segment, long position) {
        return new SegmentPosition(SegmentKeyRange.of(segment), position);
    }
}
