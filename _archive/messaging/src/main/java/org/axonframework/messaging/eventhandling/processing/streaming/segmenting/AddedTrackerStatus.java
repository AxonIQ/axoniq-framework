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
 * References a new {@link EventTrackerStatus} which work just has started on. Can be used as a marker to react on in
 * the {@link EventTrackerStatusChangeListener}.
 *
 * @author Steven van Beelen
 * @since 4.4
 */
public class AddedTrackerStatus extends WrappedTrackerStatus {

    /**
     * Initializes the {@link AddedTrackerStatus} using the given {@code addedTrackerStatus}.
     *
     * @param addedTrackerStatus the added {@link EventTrackerStatus} this implementation references
     */
    public AddedTrackerStatus(EventTrackerStatus addedTrackerStatus) {
        super(addedTrackerStatus);
    }

    @Override
    public boolean trackerAdded() {
        return true;
    }

    @Override
    public String toString() {
        return "AddedTrackerStatus{" +
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
