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

package org.axonframework.messaging.eventstreaming;

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

/**
 * An implementation of the {@link StreamingCondition} that will start
 * {@link StreamableEventSource#open(StreamingCondition) streaming} from the given {@code position}.
 *
 * @param position The {@link TrackingToken} describing the position to start streaming from.
 * @author Steven van Beelen
 * @since 5.0.0
 */
record StartingFrom(@Nullable TrackingToken position) implements StreamingCondition {

    @Override
    public StreamingCondition or(EventCriteria criteria) {
        if (position == null) {
            throw new IllegalArgumentException("The position may not be null when adding criteria to it");
        }
        return new DefaultStreamingCondition(position, criteria);
    }
}
