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
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Shared fixtures for tests partitioning workflow instances over segments: a four-segment layout, plus lookups that
 * derive segments and workflow ids from the production ownership rule in {@link WorkflowSegmentOwnership}, so a test
 * never hardcodes an id-to-segment assignment the hash could contradict.
 *
 * @author Stefan Dragisic
 */
final class SegmentTestFixtures {

    static final int SEGMENT_COUNT = 4;
    static final List<Segment> FOUR_SEGMENTS = IntStream.range(0, SEGMENT_COUNT)
                                                        .mapToObj(id -> new Segment(id, SEGMENT_COUNT - 1))
                                                        .toList();

    private SegmentTestFixtures() {
    }

    static TrackingToken token(long position) {
        return new GlobalSequenceTrackingToken(position);
    }

    /**
     * The segment of {@link #FOUR_SEGMENTS} that owns the given workflow id.
     */
    static Segment owningSegment(String workflowId) {
        return FOUR_SEGMENTS.stream()
                            .filter(segment -> WorkflowSegmentOwnership.ownedBy(segment, workflowId))
                            .findFirst()
                            .orElseThrow();
    }

    /**
     * Any segment of {@link #FOUR_SEGMENTS} other than the given one.
     */
    static Segment anotherSegmentThan(Segment segment) {
        return FOUR_SEGMENTS.stream()
                            .filter(candidate -> candidate.getSegmentId() != segment.getSegmentId())
                            .findFirst()
                            .orElseThrow();
    }

    /**
     * A workflow id owned by the given segment.
     */
    static String anyIdOn(Segment segment) {
        return IntStream.range(0, 512)
                        .mapToObj(i -> "sharded-" + i)
                        .filter(candidate -> WorkflowSegmentOwnership.ownedBy(segment, candidate))
                        .findFirst()
                        .orElseThrow();
    }

    /**
     * A workflow id owned by a different segment than the given id's owner.
     */
    static String idOnAnotherSegmentThan(String workflowId) {
        var owner = owningSegment(workflowId);
        return IntStream.range(1, 64)
                        .mapToObj(i -> "sharded-" + i)
                        .filter(candidate -> !WorkflowSegmentOwnership.ownedBy(owner, candidate))
                        .findFirst()
                        .orElseThrow();
    }
}
