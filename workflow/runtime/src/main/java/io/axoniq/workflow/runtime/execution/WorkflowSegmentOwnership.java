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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;

/**
 * The ownership rule partitioning workflow instances over the segments of a streaming event processor: a segment owns
 * the instances whose segment key it matches.
 * <p>
 * Both sides of sharding decide on this one rule. {@link WorkflowEngineSequencingPolicy} applies it while sequencing,
 * so an event is delivered to the segment owning the affected instance, and {@link WorkflowEngine} applies it before
 * acting on an instance, so an event delivered to every segment results in work at the owner only. A given
 * {@code workflowId} therefore maps to the same segment on every node and after every restart, because
 * {@code String.hashCode()} is specified by the JLS and stable across JVMs.
 *
 * @author Stefan Dragisic
 * @since 0.3.0
 */
final class WorkflowSegmentOwnership {

    private WorkflowSegmentOwnership() {
    }

    /**
     * Returns whether the given segment owns the given workflow instance, and therefore whether it may act on it.
     * <p>
     * Ownership is decided on the id's {@linkplain #segmentKey(String) segment key}, so all versions of one logical
     * workflow resolve to the same segment. Without a segment there is no partitioning to respect, which is the case
     * outside a segmented processor, and every instance is in scope.
     *
     * @param segment    the segment to test against, or {@code null} when acting outside a segmented processor
     * @param workflowId id of the workflow instance
     * @return {@code true} when the segment matches the id's segment key, or no segment is given
     */
    static boolean ownedBy(@Nullable Segment segment, @Nonnull String workflowId) {
        if (segment == null) {
            return true;
        }
        return segment.matches(segmentKey(workflowId));
    }

    /**
     * Returns the segment key of the given workflow id: the base part before the first {@code '#'}. Start placement
     * decides on base ids (see {@code WorkflowEngine#checkAndCreateNewInstance}), while stored instances may carry a
     * cross-version disambiguated id ({@code base#version}); deriving the key from the base part keeps placement,
     * ownership and sequencing consistent for every form of the id.
     *
     * @param workflowId id of the workflow instance, disambiguated or not
     * @return the segment key the id hashes with
     */
    static String segmentKey(@Nonnull String workflowId) {
        var separator = workflowId.indexOf('#');
        return separator == -1 ? workflowId : workflowId.substring(0, separator);
    }
}
