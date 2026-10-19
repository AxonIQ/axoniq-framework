/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks whether claimed workflow-engine segments have consumed the startup backlog.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal final class WorkflowEngineCatchUpSupport {

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
