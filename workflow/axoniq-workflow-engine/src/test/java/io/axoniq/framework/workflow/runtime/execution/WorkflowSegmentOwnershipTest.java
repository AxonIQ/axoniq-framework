/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.axoniq.framework.workflow.runtime.execution.SegmentTestFixtures.FOUR_SEGMENTS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link WorkflowSegmentOwnership}.
 *
 * @author Stefan Dragisic
 */
class WorkflowSegmentOwnershipTest {

    /**
     * The segments that own the given ids, as a set: its size is how much of the cluster the ids actually use.
     */
    private static Set<Integer> segmentsOf(List<String> workflowIds) {
        return workflowIds.stream()
                          .map(workflowId -> FOUR_SEGMENTS.stream()
                                                          .filter(segment -> WorkflowSegmentOwnership.ownedBy(
                                                                  segment, workflowId
                                                          ))
                                                          .findFirst()
                                                          .orElseThrow()
                                                          .getSegmentId())
                          .collect(Collectors.toCollection(TreeSet::new));
    }

    @Test
    void everyIdIsOwnedByExactlyOneSegment() {
        for (var workflowId : List.of("order-1", "order-1#2.0.0", "a#b", "#123")) {
            var owners = FOUR_SEGMENTS.stream()
                                      .filter(segment -> WorkflowSegmentOwnership.ownedBy(segment, workflowId))
                                      .count();
            assertThat(owners).as("owners of '%s'", workflowId).isEqualTo(1);
        }
    }

    /**
     * Ownership hashes the whole id, so no character is reserved: ids that differ only after a {@code '#'} spread over
     * the segments like ids with any other separator, instead of collapsing onto the segment of their shared prefix.
     */
    @Test
    void idsDifferingOnlyAfterAHashSpreadOverEverySegment() {
        var hashSeparated = IntStream.range(0, 200).mapToObj(i -> "order#" + i).toList();
        var dashSeparated = IntStream.range(0, 200).mapToObj(i -> "order-" + i).toList();

        assertThat(segmentsOf(hashSeparated)).hasSize(FOUR_SEGMENTS.size());
        assertThat(segmentsOf(dashSeparated)).hasSize(FOUR_SEGMENTS.size());
    }
}
