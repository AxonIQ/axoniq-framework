/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.eventhandling.processing.streaming.token;

import java.util.Objects;
import java.util.OptionalLong;

/**
 * A special {@link TrackingToken} that represents the latest (tail) position in an event stream.
 * <p>
 * This can be used to indicate that a processor wants to start at the newest available event,
 * skipping historical events.
 *
 * @author TeraSeo
 * @since 5.0.0
 */
final class LatestTrackingToken implements TrackingToken {

    public static final LatestTrackingToken INSTANCE = new LatestTrackingToken();
    private LatestTrackingToken() {

    }

    /**
     * The lower bound between this and another token is always the other,
     * since this represents the furthest point.
     */
    @Override
    public TrackingToken lowerBound(TrackingToken other) {
        return other;
    }

    /**
     * The upper bound is always this token, since it's the most recent.
     */
    @Override
    public TrackingToken upperBound(TrackingToken other) {
        return this;
    }

    /**
     * This token covers all others because it represents the latest point in the stream.
     */
    @Override
    public boolean covers(TrackingToken other) {
        return true;
    }

    /**
     * Optionally provides a relative position.
     * Returning Long.MAX_VALUE makes it easier for comparison or progress tracking.
     */
    @Override
    public OptionalLong position() {
        return OptionalLong.of(Long.MAX_VALUE);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LatestTrackingToken;
    }

    @Override
    public int hashCode() {
        return Objects.hash("LatestTrackingToken");
    }

    @Override
    public String toString() {
        return "LatestTrackingToken";
    }
}
