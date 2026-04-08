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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * DomainEventStream implementation that concatenates multiple streams, taking into account that some sequence
 * numbers may appear in more than one stream.
 * <p>
 * Streams are consumed in the order provided, skipping events that have sequence numbers equal or lower than the last
 * sequence number consumed from the previous stream.
 * <p>
 * This implementation is not thread safe. It should not be consumed by more than one thread at a time.
 *
 * @since 3.1.1
 */
public class ConcatenatingDomainEventStream implements DomainEventStream {
    private final LinkedList<DomainEventStream> streams;
    private final List<DomainEventStream> consumedStreams;
    private Long lastSeenSequenceNumber;

    /**
     * Initialize the stream, concatenating the given {@code streams}.
     *
     * @param streams The streams providing the elements to concatenate
     */
    public ConcatenatingDomainEventStream(DomainEventStream... streams) {
        this(Arrays.asList(streams));
    }

    /**
     * Initialize the stream, concatenating the given {@code streams}. The streams are consumed in the order the
     * collection returns them.
     *
     * @param streams The streams providing the elements to concatenate
     */
    public ConcatenatingDomainEventStream(Collection<DomainEventStream> streams) {
        this.streams = new LinkedList<>(streams);
        this.consumedStreams = new ArrayList<>();
    }

    @Override
    public DomainEventMessage peek() {
        if (!hasNext()) {
            return null;
        }
        return streams.peekFirst().peek();
    }

    @Override
    public boolean hasNext() {
        // check if there is anything to read in the current stream first
        if (!streams.isEmpty() && streams.peekFirst().hasNext()) {
            return true;
        }

        // consume any empty streams
        while (!streams.isEmpty() && !streams.peekFirst().hasNext()) {
            consumedStreams.add(streams.pollFirst());
        }

        // quick exit if we have emptied the streams
        if (streams.isEmpty()) {
            return false;
        }

        // potentially switch to a next stream, taking sequence numbers into account
        DomainEventMessage peeked = streams.peekFirst().peek();
        while (lastSeenSequenceNumber != null && peeked.getSequenceNumber() <= lastSeenSequenceNumber) {
            // consume
            while (!streams.peekFirst().hasNext()) {
                consumedStreams.add(streams.pollFirst());
                if (streams.isEmpty()) {
                    return false;
                }
            }
            streams.peekFirst().next();
            if (streams.peekFirst().hasNext()) {
                peeked = streams.peekFirst().peek();
            }
        }
        return !streams.isEmpty() && streams.peekFirst().hasNext();
    }

    @Override
    public DomainEventMessage next() {
        if (!hasNext()) {
            return null;
        }
        DomainEventMessage next = streams.peekFirst().next();
        lastSeenSequenceNumber = next.getSequenceNumber();
        return next;
    }

    @Override
    public Long getLastSequenceNumber() {
        return Stream.concat(consumedStreams.stream(), streams.stream())
                .map(DomainEventStream::getLastSequenceNumber)
                .filter(x -> !Objects.isNull(x))
                .reduce(Math::max)
                .orElse(null);
    }
}
