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

package org.axonframework.messaging.eventhandling.processing.streaming.segmenting;

/**
 * References an {@link EventTrackerStatus} which no active work is occurring on. Can be used as a marker to react on in
 * the {@link EventTrackerStatusChangeListener}.
 *
 * @author Steven van Beelen
 * @since 4.4
 */
public class RemovedTrackerStatus extends WrappedTrackerStatus {

    /**
     * Initializes the {@link RemovedTrackerStatus} using the given {@code removedTrackerStatus}.
     *
     * @param removedTrackerStatus the removed {@link EventTrackerStatus} this implementation references
     */
    public RemovedTrackerStatus(EventTrackerStatus removedTrackerStatus) {
        super(removedTrackerStatus);
    }

    @Override
    public boolean trackerRemoved() {
        return true;
    }

    @Override
    public String toString() {
        return "RemovedTrackerStatus{" +
                "segment=" + getSegment() +
                ", caughtUp=" + isCaughtUp() +
                ", replaying=" + isReplaying() +
                ", merging=" + isMerging() +
                ", errorState=" + isErrorState() +
                ", error=" + getError() +
                ", trackingToken=" + getTrackingToken() +
                ", currentPosition=" + getCurrentPosition() +
                ", resetPosition=" + getResetPosition() +
                ", mergeCompletedPosition=" + mergeCompletedPosition()
                + "}";
    }
}
