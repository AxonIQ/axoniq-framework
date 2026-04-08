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

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.Tag;

import java.util.Set;

/**
 * Exception indicating that an Event could not be appended to the event store because it contains more tags than the
 * storage engine can support.
 *
 * @author Allard Buijze
 * @since 5.0.0
 */
public class TooManyTagsOnEventMessageException extends IllegalArgumentException {

    private final EventMessage eventMessage;
    private final Set<Tag> tags;

    /**
     * Initialize the exception with given explanatory {@code message} for logging, referencing the given
     * {@code eventMessage} and {@code tags} for debug purposes.
     *
     * @param message      The message describing the exception.
     * @param eventMessage The violating message.
     * @param tags         The tags assigned to the message.
     */
    public TooManyTagsOnEventMessageException(String message, EventMessage eventMessage, Set<Tag> tags) {
        super(message);
        this.eventMessage = eventMessage;
        this.tags = tags;
    }

    /**
     * Returns the message that was rejected by the storage engine.
     *
     * @return the message that was rejected by the storage engine.
     */
    public EventMessage eventMessage() {
        return eventMessage;
    }

    /**
     * Returns the tags assigned to the message.
     *
     * @return the tags assigned to the message.
     */
    public Set<Tag> tags() {
        return tags;
    }
}
