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

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tracks whether claimed workflow-engine segments have consumed the startup backlog.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal final class WorkflowEngineCatchUpSupport {

    private final Map<Integer, Claim> claims = new ConcurrentHashMap<>();
    @Nullable
    private volatile TrackingToken startupHead;

    void initialize(@Nullable TrackingToken startupHead) {
        this.startupHead = startupHead;
    }

    boolean isCaughtUp(@Nullable Segment segment) {
        if (startupHead == null || segment == null) {
            return true;
        }
        var claim = claims.get(segment.getSegmentId());
        return claim != null && claim.caughtUp.get();
    }

    boolean startsAfterClaim(Segment segment, @Nullable TrackingToken claimedFrom) {
        var claim = new Claim();
        claims.put(segment.getSegmentId(), claim);
        var head = startupHead;
        if (head != null && claimedFrom != null && !claimedFrom.covers(head)) {
            return false;
        }
        return claim.caughtUp.compareAndSet(false, true);
    }

    boolean recordDelivery(@Nullable Segment segment, @Nullable TrackingToken deliveredToken) {
        if (segment == null) {
            return false;
        }
        var claim = claims.computeIfAbsent(segment.getSegmentId(), segmentId -> new Claim());
        claim.delivered.set(true);
        var head = startupHead;
        return deliveredToken != null && (head == null || deliveredToken.covers(head))
                && claim.caughtUp.compareAndSet(false, true);
    }

    boolean recordPosition(Segment segment, @Nullable TrackingToken position) {
        var claim = claims.get(segment.getSegmentId());
        var head = startupHead;
        return claim != null && !claim.delivered.getAndSet(false)
                && position != null && (head == null || position.covers(head))
                && claim.caughtUp.compareAndSet(false, true);
    }

    void release(Segment segment) {
        claims.remove(segment.getSegmentId());
    }

    private static final class Claim {

        private final AtomicBoolean caughtUp = new AtomicBoolean();
        private final AtomicBoolean delivered = new AtomicBoolean();
    }
}
