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

package org.axonframework.messaging.eventhandling.replay;

import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessor;
import org.axonframework.messaging.eventhandling.replay.annotation.AllowReplay;

/**
 * Type that can be used as parameter of Event Handler methods that indicates whether a message is delivered as part of
 * a replay, or in regular operations. Messages delivered as part of a replay may have been handled by this handler
 * before.
 * <p>
 * Note that this is only sensible for event handlers that are assigned to a {@link StreamingEventProcessor}. Event
 * Handlers assigned to a different mechanism, such as the {@link SubscribingEventProcessor} or certain extensions, will
 * always receive {@code ReplayStatus.REGULAR} as value.
 *
 * @author Allard Buijze
 * @see AllowReplay @AllowReplay
 * @since 3.2
 */
public enum ReplayStatus {

    /**
     * Indicates the message is delivered as part of a replay (and may have been delivered before)
     */
    REPLAY(true),
    /**
     * Indicates the message is not delivered as part of a replay (and has not been delivered before).
     */
    REGULAR(false);

    private final boolean isReplay;

    ReplayStatus(boolean isReplay) {
        this.isReplay = isReplay;
    }

    /**
     * Indicates whether this status represents a replay.
     *
     * @return {@code true} if this status indicates a replay, otherwise {@code false}
     */
    public boolean isReplay() {
        return isReplay;
    }
}
