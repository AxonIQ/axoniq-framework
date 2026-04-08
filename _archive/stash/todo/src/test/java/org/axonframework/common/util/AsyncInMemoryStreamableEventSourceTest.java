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

package org.axonframework.common.util;

import org.axonframework.messaging.eventhandling.AsyncInMemoryStreamableEventSource;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for {@link AsyncInMemoryStreamableEventSource}.
 */
class AsyncInMemoryStreamableEventSourceTest {

    private ProcessingContext processingContext = null;

    @Nested
    class TrackingTokenHandling {

        @Test
        @DisplayName("Starting from null token reads all events from beginning")
        void nullTrackingTokenStartsFromBeginning() {
            // Given
            AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource();

            EventMessage event1 = EventTestUtils.asEventMessage("Event 1");
            EventMessage event2 = EventTestUtils.asEventMessage("Event 2");

            eventSource.publishMessage(event1);
            eventSource.publishMessage(event2);

            // When - Start stream with null token
            StreamingCondition condition = StreamingCondition.startingFrom(null);

            // Then - Should read all events from position 1
            MessageStream<EventMessage> stream = eventSource.open(condition, processingContext);
            Optional<MessageStream.Entry<EventMessage>> entry1 = stream.next();
            assertTrue(entry1.isPresent());
            assertEquals("Event 1", entry1.get().message().payload());
            assertEquals(1, TrackingToken.fromContext(entry1.get()).orElseThrow().position().orElse(-1));

            Optional<MessageStream.Entry<EventMessage>> entry2 = stream.next();
            assertTrue(entry2.isPresent());
            assertEquals("Event 2", entry2.get().message().payload());
            assertEquals(2, TrackingToken.fromContext(entry2.get()).orElseThrow().position().orElse(-1));
        }

        @Test
        @DisplayName("Starting from token beyond available events returns no events")
        void trackingTokenBeyondAvailableEvents() {
            // Given
            AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource();

            EventMessage event1 = EventTestUtils.asEventMessage("Event 1");
            eventSource.publishMessage(event1);

            // When - Start from token position 5 (beyond available events)
            TrackingToken futureToken = new GlobalSequenceTrackingToken(5);
            StreamingCondition condition = StreamingCondition.startingFrom(futureToken);

            // Then - Should have no events available
            MessageStream<EventMessage> stream = eventSource.open(condition, processingContext);
            assertFalse(stream.hasNextAvailable(),
                        "Stream should have no events when starting beyond available events");
            assertTrue(stream.next().isEmpty(), "next() should return empty Optional");
        }
    }
}