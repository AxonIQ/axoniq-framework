/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges.SegmentPosition;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;

/**
 * The rows of one segment in a {@link WorkflowIndex}. Internal, because the {@link WorkflowEngine} uses it to restore
 * the workflows of a claimed segment.
 *
 * @param runningWorkflowIds the identifiers of the running workflows of the segment
 * @param wakesAt            the time each hibernated workflow wakes at, by workflow identifier
 * @param waits              the open waits of the segment
 * @param positions          the token positions of the segments of the module, for the workflow manager; empty for
 *                           the rows of a claim
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public record WorkflowIndexRows(Set<String> runningWorkflowIds,
                                Map<String, Instant> wakesAt,
                                Collection<WaitAssociation> waits,
                                List<SegmentPosition> positions) {

    /**
     * Creates the rows of a segment, copying the given collections.
     *
     * @param runningWorkflowIds the identifiers of the running workflows of the segment
     * @param wakesAt            the time each hibernated workflow wakes at, by workflow identifier
     * @param waits              the open waits of the segment
     * @param positions          the token positions of the segments of the module, for the workflow manager
     */
    public WorkflowIndexRows {
        runningWorkflowIds = Set.copyOf(
                Objects.requireNonNull(runningWorkflowIds, "The running workflow ids must not be null.")
        );
        wakesAt = Map.copyOf(Objects.requireNonNull(wakesAt, "The wake times must not be null."));
        waits = List.copyOf(Objects.requireNonNull(waits, "The waits must not be null."));
        positions = List.copyOf(Objects.requireNonNull(positions, "The positions must not be null."));
    }

    /**
     * Creates the rows of a segment for a claim, without positions.
     *
     * @param runningWorkflowIds the identifiers of the running workflows of the segment
     * @param wakesAt            the time each hibernated workflow wakes at, by workflow identifier
     * @param waits              the open waits of the segment
     */
    public WorkflowIndexRows(Set<String> runningWorkflowIds,
                             Map<String, Instant> wakesAt,
                             Collection<WaitAssociation> waits) {
        this(runningWorkflowIds, wakesAt, waits, List.of());
    }

    /**
     * Returns the position the workflow manager reads the events of the given segment from: the lowest of the
     * positions that hold its segment keys, or empty when a key has no position, as after a split before the new half
     * stored its token.
     *
     * @param segment the segment to read the events of, usually the root segment of the whole module
     * @return the position to read from, or empty when a key of the segment has no position
     */
    public OptionalLong positionOf(Segment segment) {
        var range = SegmentKeyRange.of(segment);
        long lowest = Long.MAX_VALUE;
        long key = range.from();
        while (key <= range.to()) {
            boolean held = false;
            long highest = Long.MIN_VALUE;
            // The last key before the set of ranges that hold a key changes.
            long end = range.to();
            for (SegmentPosition stored : positions) {
                if (stored.range().contains(key)) {
                    held = true;
                    highest = Math.max(highest, stored.position());
                    end = Math.min(end, stored.range().to());
                } else if (stored.range().from() > key) {
                    end = Math.min(end, stored.range().from() - 1);
                }
            }
            if (!held) {
                return OptionalLong.empty();
            }
            lowest = Math.min(lowest, highest);
            key = end + 1;
        }
        return OptionalLong.of(lowest);
    }
}
