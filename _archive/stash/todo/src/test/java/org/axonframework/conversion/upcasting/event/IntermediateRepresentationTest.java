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

package org.axonframework.conversion.upcasting.event;

import org.axonframework.messaging.eventhandling.DomainEventMessage;
import org.axonframework.messaging.eventhandling.EventData;
import org.axonframework.messaging.eventhandling.GenericDomainEventMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.Serializer;
import org.axonframework.conversion.TestSerializer;
import org.axonframework.util.TestDomainEventEntry;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test for intermediate representation.
 *
 * @author Simon Zambrovski
 */
class IntermediateRepresentationTest {

    private final static Serializer serializer = TestSerializer.JACKSON.getSerializer();

//    @Test
//    public void canConvertDataTo() {
//        DomainEventMessage testEvent = new GenericDomainEventMessage(
//                "test", "aggregateId", 0, new MessageType("event"), "someString"
//        );
//        EventData<?> eventData = new TestDomainEventEntry(testEvent, serializer);
//        Serializer serializer = mock(Serializer.class);
//        Converter converter = mock(Converter.class);
//        when(serializer.getConverter()).thenReturn(converter);
//        when(converter.canConvert(any(), eq(String.class))).thenReturn(true);
//
//        IntermediateEventRepresentation input = new InitialEventRepresentation(eventData, serializer);
//        EventUpcasterChain eventUpcasterChain = new EventUpcasterChain(
//                new IntermediateRepresentationTest.MyEventUpcaster()
//        );
//        List<IntermediateEventRepresentation> result = eventUpcasterChain.upcast(Stream.of(input)).toList();
//        assertEquals(1, result.size());
//
//
//        assertTrue(input.canConvertDataTo(String.class));
//        assertTrue(result.getFirst().canConvertDataTo(String.class));
//
//        verify(converter).canConvert(String.class, String.class);
//    }

    private static class MyEventUpcaster extends SingleEventUpcaster {

        @Override
        protected boolean canUpcast(IntermediateEventRepresentation intermediateRepresentation) {
            return true;
        }

        @Override
        protected IntermediateEventRepresentation doUpcast(IntermediateEventRepresentation intermediateRepresentation) {
            return new UpcastedEventRepresentation<>(
                    intermediateRepresentation.getType(),
                    intermediateRepresentation,
                    Function.identity(),
                    Function.identity(),
                    Object.class,
                    serializer.getConverter()
            );
        }
    }
}
