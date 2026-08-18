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
package io.axoniq.workflow.runtime.execution;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.FOUR_SEGMENTS;
import static org.assertj.core.api.Assertions.assertThat;

class SegmentedWorkflowRoutingTest {


    @Test
    void everyIdIsOwnedByExactlyOneSegment() {
        for (var workflowId : List.of("order-1", "order-1#2.0.0", "a#b", "")) {
            var owners = FOUR_SEGMENTS.stream()
                                      .filter(segment -> SegmentedWorkflowRouting.ownedBy(segment, workflowId))
                                      .count();
            assertThat(owners).as("owners of '%s'", workflowId).isEqualTo(1);
        }
    }

    @Test
    void crossVersionDisambiguatedIdsShareTheSegmentOfTheirBaseId() {
        IntStream.range(0, 200).forEach(i -> {
            var baseId = "order-" + i;
            var disambiguated = baseId + "#2.0.0";
            for (var segment : FOUR_SEGMENTS) {
                assertThat(SegmentedWorkflowRouting.ownedBy(segment, disambiguated))
                        .as("segment %s must own '%s' iff it owns '%s'", segment, disambiguated, baseId)
                        .isEqualTo(SegmentedWorkflowRouting.ownedBy(segment, baseId));
            }
        });
    }

    /**
     * The flip side of the test above, and the reason {@code '#'} is a reserved character rather than an
     * implementation detail: the segment key is everything before the first {@code '#'}, so an application whose
     * {@code workflowIdProvider} returns {@code order#123} hands every order the same segment key and runs the whole
     * workload on one segment. Nothing rejects such an id and nothing warns about it, so a sharded processor degrades
     * to a single segment while every count, claim row and health check still looks right. The remaining segments stay
     * claimed and idle, which is why this presents as a throughput problem rather than as an error.
     */
    @Test
    void everyIdSharingItsBasePartCollapsesOntoOneSegment() {
        var collapsing = IntStream.range(0, 200).mapToObj(i -> "order#" + i).toList();
        var spreading = IntStream.range(0, 200).mapToObj(i -> "order-" + i).toList();

        assertThat(segmentsOf(collapsing))
                .as("""
                    Segments used by 200 ids of the shape 'order#<n>': %s. The segment key stops at the first '#', so \
                    all 200 share the key 'order' and one of the %d segments does all the work. '#' is a common \
                    identifier separator and its reservation is neither validated nor documented.""",
                    segmentsOf(collapsing), FOUR_SEGMENTS.size())
                .hasSize(1);
        assertThat(segmentsOf(spreading))
                .as("the same ids with a separator that is not reserved spread over every segment, which is what "
                            + "makes the collapse above a property of '#' and not of the ids")
                .hasSize(FOUR_SEGMENTS.size());
    }

    /**
     * An id that starts with {@code '#'} has the empty string as its segment key, whose hash is zero, so it lands on
     * segment 0 under every mask. Two consequences worth stating separately from the collapse above: segment 0 is
     * where such ids pile up on any segment count, and the id is not rejected on the way in.
     */
    @Test
    void anIdStartingWithTheSeparatorAlwaysLandsOnSegmentZero() {
        for (var workflowId : List.of("#123", "#", "#a#b")) {
            assertThat(SegmentedWorkflowRouting.ownedBy(FOUR_SEGMENTS.getFirst(), workflowId))
                    .as("'%s' has the empty segment key, and hash 0 masks to segment 0 for every mask", workflowId)
                    .isTrue();
        }
    }

    /** The segments that own the given ids, as a set: its size is how much of the cluster the ids actually use. */
    private static Set<Integer> segmentsOf(List<String> workflowIds) {
        return workflowIds.stream()
                          .map(workflowId -> FOUR_SEGMENTS.stream()
                                                          .filter(segment -> SegmentedWorkflowRouting.ownedBy(segment,
                                                                                                             workflowId))
                                                          .findFirst()
                                                          .orElseThrow()
                                                          .getSegmentId())
                          .collect(Collectors.toCollection(TreeSet::new));
    }
}
