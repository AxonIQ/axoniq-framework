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
import org.axonframework.messaging.eventhandling.GenericDomainEventMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.messaging.eventsourcing.eventstore.FilteringDomainEventStream;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FilteringDomainEventStreamTest {

    private DomainEventMessage event1;
    private DomainEventMessage event2;
    private DomainEventMessage event3;

    @BeforeEach
    void setUp() throws Exception {
        event1 = new GenericDomainEventMessage("type", "1", 0L,
                                                 new MessageType("event"), "Create type 1");
        event2 = new GenericDomainEventMessage("type2", "1", 0L,
                                                 new MessageType("event"), "Create type 2");
        event3 = new GenericDomainEventMessage("type2", "1", 1L,
                                                 new MessageType("event"), "Change type 2");
    }

    @Test
    void forEachRemainingType1() {
        List<DomainEventMessage> expectedMessages = Collections.singletonList(event1);

        DomainEventStream concat = new FilteringDomainEventStream(
                DomainEventStream.of(event1, event2, event3), // Initial stream - add all elements
                e -> e.getType().equals("type")
        );

        List<DomainEventMessage> actualMessages = new ArrayList<>();
        concat.forEachRemaining(actualMessages::add);

        assertEquals(expectedMessages, actualMessages);
    }

    @Test
    void forEachRemainingType2() {
        List<DomainEventMessage> expectedMessages = Arrays.asList(event2, event3);

        DomainEventStream concat = new FilteringDomainEventStream(
                DomainEventStream.of(event1, event2, event3), // Initial stream - add all elements
                e -> e.getType().equals("type2")
        );

        List<DomainEventMessage> actualMessages = new ArrayList<>();
        concat.forEachRemaining(actualMessages::add);

        assertEquals(expectedMessages, actualMessages);
    }
}
