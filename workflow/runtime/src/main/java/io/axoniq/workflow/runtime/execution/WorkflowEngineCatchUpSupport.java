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

import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks whether claimed workflow-engine segments have consumed the startup backlog.
 *
 * @author Simon Zambrovski
 * @since 0.3.0
 */
@Internal
final class WorkflowEngineCatchUpSupport {

    private final Set<Integer> caughtUpSegments = ConcurrentHashMap.newKeySet();
    @Nullable
    private volatile TrackingToken startupHead;

    void initialize(@Nullable TrackingToken startupHead) {
        this.startupHead = startupHead;
    }

    boolean isCaughtUp(@Nullable Segment segment) {
        return startupHead == null || segment == null || caughtUpSegments.contains(segment.getSegmentId());
    }

    boolean startsAfterClaim(Segment segment, @Nullable TrackingToken claimedFrom) {
        var head = startupHead;
        if (head != null && claimedFrom != null && !claimedFrom.covers(head)
                && !caughtUpSegments.contains(segment.getSegmentId())) {
            return false;
        }
        return caughtUpSegments.add(segment.getSegmentId());
    }

    boolean recordDelivery(@Nullable Segment segment, @Nullable TrackingToken deliveredToken) {
        var head = startupHead;
        return segment != null && deliveredToken != null && (head == null || deliveredToken.covers(head))
                && caughtUpSegments.add(segment.getSegmentId());
    }

    void release(Segment segment) {
        caughtUpSegments.remove(segment.getSegmentId());
    }
}
