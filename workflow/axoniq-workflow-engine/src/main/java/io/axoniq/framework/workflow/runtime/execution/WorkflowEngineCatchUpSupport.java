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
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.util.Map;
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
    // Per segment that waits for catch-up, the handle of the claim that waits, so a wait outliving its claim ends.
    private final Map<Integer, Object> waits = new ConcurrentHashMap<>();
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

    /**
     * Registers that the current claim of the given {@code segment} waits for catch-up, and returns the handle of that
     * wait for {@link #isWaiting(Segment, Object)}.
     */
    Object startWaiting(Segment segment) {
        Object wait = new Object();
        waits.put(segment.getSegmentId(), wait);
        return wait;
    }

    /**
     * Returns whether the given {@code wait} still waits for the given {@code segment} to catch up: the segment has not
     * caught up yet, and was not released or claimed again since the wait started.
     */
    boolean isWaiting(Segment segment, Object wait) {
        return waits.get(segment.getSegmentId()) == wait && !caughtUpSegments.contains(segment.getSegmentId());
    }

    void release(Segment segment) {
        waits.remove(segment.getSegmentId());
        caughtUpSegments.remove(segment.getSegmentId());
    }
}
