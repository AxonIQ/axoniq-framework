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

package org.axonframework.eventsourcing.eventstore;

import org.axonframework.common.AxonNonTransientException;
import org.axonframework.messaging.eventstreaming.EventCriteria;

/**
 * Exception indicating that a transaction was rejected due to conflicts detected in the events to append.
 *
 * @author Steven van Beelen
 * @author Allard Buijze
 * @since 5.0.0
 */
public class AppendEventsTransactionRejectedException extends AxonNonTransientException {

    /**
     * Constructs an {@code AppendConditionAssertionException} with the given {@code message}.
     *
     * @param message The message of the {@code AppendConditionAssertionException} under construction.
     */
    public AppendEventsTransactionRejectedException(String message) {
        super(message);
    }

    /**
     * Constructs an {@code AppendConditionAssertionException} noting that the {@link EventStorageEngine} contains
     * events matching the {@link AppendCondition#criteria() criteria} passed the given {@code consistencyMarker}.
     *
     * @param consistencyMarker The pointer in the {@link EventStorageEngine} after which no events should've been
     *                          appended that match the {@link EventCriteria} of an {@link AppendCondition}.
     * @return An {@code AppendConditionAssertionException} noting that the {@link EventStorageEngine} contains events
     * matching the {@link AppendCondition#criteria() criteria} passed the given {@code consistencyMarker}.
     */
    public static AppendEventsTransactionRejectedException conflictingEventsDetected(
            ConsistencyMarker consistencyMarker
    ) {
        return new AppendEventsTransactionRejectedException(
                "Event matching append criteria have been detected beyond provided consistency marker: "
                        + consistencyMarker
        );
    }
}
