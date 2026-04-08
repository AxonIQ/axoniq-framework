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

import org.axonframework.messaging.eventhandling.DomainEventMessage;
import org.axonframework.messaging.eventhandling.GenericDomainEventMessage;
import org.axonframework.messaging.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;

import java.util.NoSuchElementException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link DomainEventStream}.
 *
 * @author Allard Buijze
 */
class DomainEventStreamTest {

    @Test
    void peek() {
        DomainEventMessage event1 = new GenericDomainEventMessage(
                "type", UUID.randomUUID().toString(), 0L, new MessageType("event"), "Mock contents"
        );
        DomainEventMessage event2 = new GenericDomainEventMessage(
                "type", UUID.randomUUID().toString(), 0L, new MessageType("event"), "Mock contents"
        );
        DomainEventStream testSubject = DomainEventStream.of(event1, event2);
        assertSame(event1, testSubject.peek());
        assertSame(event1, testSubject.peek());
    }

    @Test
    void peek_EmptyStream() {
        DomainEventStream testSubject = DomainEventStream.of();
        assertFalse(testSubject.hasNext());
        try {
            testSubject.peek();
            fail("Expected NoSuchElementException");
        } catch (NoSuchElementException e) {
            // what we expect
        }
    }

    @Test
    void nextAndHasNext() {
        DomainEventMessage event1 = new GenericDomainEventMessage(
                "type", UUID.randomUUID().toString(), 0L, new MessageType("event"), "Mock contents"
        );
        DomainEventMessage event2 = new GenericDomainEventMessage(
                "type", UUID.randomUUID().toString(), 0L, new MessageType("event"), "Mock contents"
        );
        DomainEventStream testSubject = DomainEventStream.of(event1, event2);
        assertTrue(testSubject.hasNext());
        assertSame(event1, testSubject.next());
        assertTrue(testSubject.hasNext());
        assertSame(event2, testSubject.next());
        assertFalse(testSubject.hasNext());
    }

    @Test
    void next_ReadBeyondEnd() {
        DomainEventMessage event = new GenericDomainEventMessage(
                "type", UUID.randomUUID().toString(), 0L, new MessageType("event"), "Mock contents"
        );
        DomainEventStream testSubject = DomainEventStream.of(event);
        testSubject.next();

        assertThrows(NoSuchElementException.class, testSubject::next);
    }
}
