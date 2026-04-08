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

package org.axonframework.messaging.eventsourcing;

import org.axonframework.common.IdentifierFactory;
import org.axonframework.messaging.eventhandling.DomainEventData;
import org.axonframework.messaging.eventhandling.GenericDomainEventEntry;
import org.axonframework.messaging.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.conversion.Serializer;
import org.axonframework.conversion.upcasting.event.EventUpcasterChain;
import org.axonframework.conversion.upcasting.event.NoOpEventUpcaster;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Objects;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link EventStreamUtils}.
 *
 * @author Rene de Waele
 */
class EventStreamUtilsTest {

    private Serializer serializer;

    @BeforeEach
    void setUp() {
        serializer = mock(Serializer.class);

        //noinspection deprecation
        when(serializer.classForType(any())).thenReturn(EventStreamUtilsTest.class);
    }

    @Test
    void domainEventStream_lastSequenceNumberEqualToLastProcessedEntry() {
        DomainEventStream eventStream = EventStreamUtils.upcastAndDeserializeDomainEvents(
                Stream.of(createEntry(1)), serializer, NoOpEventUpcaster.INSTANCE
        );

        assertNull(eventStream.getLastSequenceNumber());
        eventStream.forEachRemaining(Objects::requireNonNull);
        assertEquals(Long.valueOf(1L), eventStream.getLastSequenceNumber());
    }

    @Test
    void domainEventStream_lastSequenceNumberEqualToLastProcessedEntryAfterIgnoringLastEntry() {
        DomainEventStream eventStream = EventStreamUtils.upcastAndDeserializeDomainEvents(
                Stream.of(createEntry(1), createEntry(2), createEntry(3)), serializer,
                new EventUpcasterChain(e -> e.filter(entry -> entry.getSequenceNumber().get() < 2L))
        );

        assertNull(eventStream.getLastSequenceNumber());
        assertTrue(eventStream.hasNext());
        eventStream.forEachRemaining(Objects::requireNonNull);
        assertEquals(Long.valueOf(3L), eventStream.getLastSequenceNumber());
    }

    @Test
    void domainEventStream_lastSequenceNumberEqualToLastProcessedEntryAfterUpcastingToEmptyStream() {
        DomainEventStream eventStream = EventStreamUtils.upcastAndDeserializeDomainEvents(
                Stream.of(createEntry(1)), serializer, new EventUpcasterChain(s -> s.filter(e -> false))
        );

        assertNull(eventStream.getLastSequenceNumber());
        assertFalse(eventStream.hasNext());
        eventStream.forEachRemaining(Objects::requireNonNull);
        assertEquals(Long.valueOf(1L), eventStream.getLastSequenceNumber());
    }

    @Test
    void domainEventStream_nullPointerExceptionOnEmptyEventStream() {
        DomainEventStream eventStream = EventStreamUtils.upcastAndDeserializeDomainEvents(
                Stream.empty(), serializer, NoOpEventUpcaster.INSTANCE
        );

        assertNull(eventStream.getLastSequenceNumber());
    }

    private static DomainEventData<?> createEntry(long sequenceNumber) {
        return new GenericDomainEventEntry<>("type", "testAggregate", sequenceNumber,
                                             IdentifierFactory.getInstance().generateIdentifier(), Instant.now(),
                                             String.class.getName(), null, "test", "metadata");
    }
}
