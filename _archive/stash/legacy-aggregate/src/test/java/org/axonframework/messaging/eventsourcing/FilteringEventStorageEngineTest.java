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
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.GenericDomainEventMessage;
import org.axonframework.messaging.eventsourcing.LegacyFilteringEventStorageEngine;
import org.axonframework.messaging.eventsourcing.eventstore.LegacyEventStorageEngine;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.*;

class FilteringEventStorageEngineTest {

    private LegacyEventStorageEngine mockStorage;
    private LegacyFilteringEventStorageEngine testSubject;

    @BeforeEach
    void setUp() {
        Predicate<EventMessage> filter = m -> m.payload().toString().contains("accept");
        mockStorage = mock(LegacyEventStorageEngine.class);
        testSubject = new LegacyFilteringEventStorageEngine(mockStorage, filter);
    }

    @Test
    void eventsFromArrayMatchingAreForwarded() {
        EventMessage event1 = EventTestUtils.asEventMessage("accept");
        EventMessage event2 = EventTestUtils.asEventMessage("fail");
        EventMessage event3 = EventTestUtils.asEventMessage("accept");

        testSubject.appendEvents(event1, event2, event3);

        verify(mockStorage).appendEvents(asList(event1, event3));
    }

    @Test
    void eventsFromListMatchingAreForwarded() {
        EventMessage event1 = EventTestUtils.asEventMessage("accept");
        EventMessage event2 = EventTestUtils.asEventMessage("fail");
        EventMessage event3 = EventTestUtils.asEventMessage("accept");

        testSubject.appendEvents(asList(event1, event2, event3));

        verify(mockStorage).appendEvents(asList(event1, event3));
    }

    @Test
    void storeSnapshotDelegated() {
        DomainEventMessage snapshot = new GenericDomainEventMessage(
                "type", "id", 0, new MessageType("snapshot"), "fail"
        );
        testSubject.storeSnapshot(snapshot);

        verify(mockStorage).storeSnapshot(snapshot);
    }

    @Test
    void createTailTokenDelegated() {
        testSubject.createTailToken();

        verify(mockStorage).createTailToken();
    }

    @Test
    void createHeadTokenDelegated() {
        testSubject.createHeadToken();

        verify(mockStorage).createHeadToken();
    }

    @Test
    void createTokenAtDelegated() {
        Instant now = Instant.now();
        testSubject.createTokenAt(now);

        verify(mockStorage).createTokenAt(now);
    }
}
