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

package org.axonframework.eventsourcing;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.modelling.PayloadBasedEntityEvolver;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link PayloadBasedEntityEvolver}.
 *
 * @author Mitchell Herrijgers
 */
class PayloadBasedEntityEvolverTest {

    private static final String ENTITY = "base";
    private static final String PAYLOAD = ":test-event";

    private PayloadBasedEntityEvolver<String, String> testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new PayloadBasedEntityEvolver<>(String.class, (entity, event) -> entity + event);
    }

    @Test
    void evolveAppliesEventWhenPayloadTypeIsConvertible() {
        EventMessage testEvent = new GenericEventMessage(new MessageType(String.class), PAYLOAD);

        String result = testSubject.evolve(ENTITY, testEvent, StubProcessingContext.forMessage(testEvent));

        assertEquals(ENTITY + PAYLOAD, result);
    }

    @Test
    void evolveThrowsExceptionWhenPayloadTypeCannotBeConverted() {
        EventMessage testEvent =
                new GenericEventMessage(new MessageType(String.class), new StringBuilder());

        assertThrows(ClassCastException.class, () -> testSubject.evolve(ENTITY, testEvent, StubProcessingContext.forMessage(testEvent)));
    }

    @Test
    void evolveIsInvokedSuccessfullyRegardlessOfQualifiedNameMismatch() {
        EventMessage testEvent = new GenericEventMessage(new MessageType(Integer.class), PAYLOAD);

        String result = testSubject.evolve(ENTITY, testEvent, StubProcessingContext.forMessage(testEvent));

        assertEquals(ENTITY + PAYLOAD, result);
    }

    @Test
    void evolveThrowsNullPointerExceptionForNullEntity() {
        EventMessage testEvent = new GenericEventMessage(new MessageType(String.class), PAYLOAD);

        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> testSubject.evolve(null, testEvent, StubProcessingContext.forMessage(testEvent)));
    }

    @Test
    void evolveThrowsNullPointerExceptionForNullEvent() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> testSubject.evolve(ENTITY, null, null));
    }

    @Test
    void throwsExceptionIfNullPayloadTypeIsSupplied() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new PayloadBasedEntityEvolver<String, String>(
                null,
                (entity, event) -> entity + event
        ));
    }

    @Test
    void throwsExceptionIfNullEventStateApplierIsSupplied() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new PayloadBasedEntityEvolver<String, String>(
                String.class,
                null
        ));
    }
}