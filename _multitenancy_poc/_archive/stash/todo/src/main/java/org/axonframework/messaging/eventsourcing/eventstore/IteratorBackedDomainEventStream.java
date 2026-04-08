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

package org.axonframework.messaging.eventsourcing.eventstore;

import org.axonframework.messaging.eventhandling.DomainEventMessage;

import java.util.Iterator;

/**
 * DomainEventStream implementation that gets its messages from an Iterator.
 *
 * @since 3.0.3
 */
public class IteratorBackedDomainEventStream implements DomainEventStream {
    private final Iterator<? extends DomainEventMessage> iterator;
    private boolean hasPeeked;
    private DomainEventMessage peekEvent;
    private Long sequenceNumber;

    /**
     * Initialize the stream which provides access to message from the given {@code iterator}
     *
     * @param iterator The iterator providing the messages to stream
     */
    public IteratorBackedDomainEventStream(Iterator<? extends DomainEventMessage> iterator) {
        this.iterator = iterator;
    }

    @Override
    public DomainEventMessage peek() {
        if (!hasPeeked) {
            peekEvent = readNext();
            hasPeeked = true;
        }
        return peekEvent;
    }

    @Override
    public boolean hasNext() {
        return hasPeeked || iterator.hasNext();
    }

    @Override
    public DomainEventMessage next() {
        if (!hasPeeked) {
            return readNext();
        }
        DomainEventMessage result = peekEvent;
        peekEvent = null;
        hasPeeked = false;
        return result;
    }

    private DomainEventMessage readNext() {
        DomainEventMessage next = iterator.next();
        this.sequenceNumber = next.getSequenceNumber();
        return next;
    }

    @Override
    public Long getLastSequenceNumber() {
        return sequenceNumber;
    }
}
