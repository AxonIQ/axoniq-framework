/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.StringUtils;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

/**
 * The ownership rule partitioning workflow instances over the segments of a streaming event processor: a segment owns
 * the instances whose id it matches.
 * <p>
 * Both sides of sharding decide on this one rule. {@link WorkflowEngineSequencingPolicy} applies it while sequencing,
 * so an event is delivered to the segment owning the affected instance, and {@link WorkflowEngine} applies it before
 * acting on an instance, so an event delivered to every segment results in work at the owner only. A given
 * {@code workflowId} therefore maps to the same segment on every node and after every restart, because
 * {@code String.hashCode()} is specified by the JLS and stable across JVMs.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
final class WorkflowSegmentOwnership {

    private WorkflowSegmentOwnership() {
    }

    /**
     * Returns whether the given segment owns the given workflow instance, and therefore whether it may act on it.
     * <p>
     * Ownership is decided on the whole id. Without a segment there is no partitioning to respect, which is the case
     * outside a segmented processor, and every instance is in scope.
     *
     * @param segment    the segment to test against, or {@code null} when acting outside a segmented processor
     * @param workflowId id of the workflow instance
     * @return {@code true} when the segment matches the id, or no segment is given
     */
    static boolean ownedBy(@Nullable Segment segment, String workflowId) {
        if (segment == null) {
            return true;
        }
        if (StringUtils.emptyOrNull(workflowId)) {
            throw new IllegalArgumentException(
                    "Cannot decide segment ownership without a workflow id; configure an idProperty for the workflow."
            );
        }
        return segment.matches(workflowId);
    }

    /**
     * Returns whether the given segments share any workflow instance: whether one of them contains the other. A
     * processor never holds two overlapping segments at once, so state kept for a segment that overlaps a segment being
     * claimed or released belongs to a segment that was split or merged without a release.
     *
     * @param one   a segment
     * @param other another segment
     * @return {@code true} when the segments share workflow instances
     */
    static boolean overlap(Segment one, Segment other) {
        int commonMask = one.getMask() & other.getMask();
        return (one.getSegmentId() & commonMask) == (other.getSegmentId() & commonMask);
    }
}
