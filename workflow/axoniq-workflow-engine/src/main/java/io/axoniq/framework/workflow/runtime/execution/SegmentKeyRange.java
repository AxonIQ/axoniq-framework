/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;

import java.util.Objects;

/**
 * The range of segment keys a {@link Segment} owns, so a store finds the workflow instances of a segment with one range
 * query on an indexed column.
 * <p>
 * A {@link Segment} owns the workflow ids whose hash ends in the bits of its segment id, see
 * {@link Segment#matches(Object)}. The segment key of a workflow id is that hash with its bits reversed, read as an
 * unsigned number. Reversed, the ids of one segment form one contiguous range of keys. A split halves the range and a
 * merge joins two neighbouring ranges, so the key stored with a row never changes.
 * <p>
 * This is the rule of {@link WorkflowSegmentOwnership} in a form a database can index: a key lies in the range of a
 * segment exactly when that segment owns the workflow id.
 * <p>
 * Internal, because it is part of how a {@link WorkflowIndex} stores and selects its rows.
 *
 * @param from the lowest segment key the segment owns
 * @param to   the highest segment key the segment owns
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public record SegmentKeyRange(long from, long to) {

    private static final long HIGHEST_KEY = 0xFFFF_FFFFL;

    /**
     * Returns the segment key of the given workflow id: its hash with the bits reversed, as an unsigned number.
     *
     * @param workflowId the identifier of the workflow instance
     * @return the segment key, between 0 and 2<sup>32</sup> - 1
     */
    public static long keyOf(String workflowId) {
        Objects.requireNonNull(workflowId, "The workflow id must not be null.");
        return Integer.toUnsignedLong(Integer.reverse(workflowId.hashCode()));
    }

    /**
     * Returns the range of segment keys the given segment owns.
     *
     * @param segment the segment to return the range of
     * @return the range holding the key of every workflow id the segment matches, and of no other
     */
    public static SegmentKeyRange of(Segment segment) {
        Objects.requireNonNull(segment, "The Segment must not be null.");
        long from = Integer.toUnsignedLong(Integer.reverse(segment.getSegmentId()));
        return new SegmentKeyRange(from, from + (HIGHEST_KEY >>> Integer.bitCount(segment.getMask())));
    }

    /**
     * Returns whether the given segment key lies in this range.
     *
     * @param segmentKey the segment key to test
     * @return {@code true} when the key lies between {@link #from()} and {@link #to()}, both included
     */
    public boolean contains(long segmentKey) {
        return from <= segmentKey && segmentKey <= to;
    }
}
