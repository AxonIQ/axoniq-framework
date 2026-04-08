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

import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;

import java.util.Map;

/**
 * Represent a listener that is notified whenever the {@link Map} of {@link EventTrackerStatus}' in the {@link
 * StreamingEventProcessor} this component is registered to has changed.
 *
 * @author Steven van Beelen
 * @since 4.4
 */
public interface EventTrackerStatusChangeListener {

    /**
     * Notification that an {@link EventTrackerStatus} has changed. Implementations should take into account that this
     * listener may be called concurrently, and that multiple invocations do not necessarily reflect the order in which
     * changes have occurred. If this order is important to the implementation, it should verify the {@link Segment} and
     * the status' positions of the given {@code updatedTrackerStatus} through the {@link
     * EventTrackerStatus#getSegment()} and {@link EventTrackerStatus#getCurrentPosition()} methods respectively.
     *
     * @param updatedTrackerStatus the updated {@link EventTrackerStatus} to react on
     */
    void onEventTrackerStatusChange(Map<Integer, EventTrackerStatus> updatedTrackerStatus);

    /**
     * Flag dictating whether an {@link EventTrackerStatus}'s positions (e.g. {@link
     * EventTrackerStatus#getCurrentPosition()}) should be taken into account as a change to listen for. Defaults to
     * {@code false}, as positions typically change a lot in a running system.
     *
     * @return {@code true} if positions should be validated, {@code false} otherwise.
     */
    default boolean validatePositions() {
        return false;
    }

    /**
     * Returns a no-op implementation of the {@link EventTrackerStatusChangeListener}.
     *
     * @return a no-op implementation of the {@link EventTrackerStatusChangeListener}
     */
    static EventTrackerStatusChangeListener noOp() {
        return ignored -> {
        };
    }
}
