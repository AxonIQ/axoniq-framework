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

package org.axonframework.test.saga;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.LegacyMessageSupportingContext;
import org.axonframework.test.AxonAssertionError;
import org.axonframework.test.matchers.AllFieldsFilter;
import org.axonframework.test.matchers.Matchers;
import org.axonframework.common.util.StubDomainEvent;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class EventValidatorTest {

    private EventValidator testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new EventValidator(null, AllFieldsFilter.instance());
    }

    @Test
    void assertPublishedEventsWithNoEventsMatcherIfNoEventWasPublished() {
        testSubject.assertPublishedEventsMatching(Matchers.noEvents());
    }

    private static <P> EventMessage asEventMessage(P event) {
        return new GenericEventMessage(
                new GenericMessage(new MessageType(event.getClass()), (P) event),
                () -> GenericEventMessage.clock.instant()
        );
    }

    @Test
    void assertPublishedEventsIfNoEventWasPublished() {
        testSubject.assertPublishedEvents();
    }

    @Test
    void assertPublishedEventsWithNoEventsMatcherThrowsAssertionErrorIfEventWasPublished() {
        EventMessage eventMessage = asEventMessage(new StubDomainEvent());
        testSubject.handleSync(eventMessage, new LegacyMessageSupportingContext(eventMessage));

        assertThrows(AxonAssertionError.class, () -> testSubject.assertPublishedEventsMatching(Matchers.noEvents()));
    }

    @Test
    void assertPublishedEventsThrowsAssertionErrorIfEventWasPublished() {
        EventMessage eventMessage = asEventMessage(new StubDomainEvent());
        testSubject.handleSync(eventMessage, new LegacyMessageSupportingContext(eventMessage));

        assertThrows(AxonAssertionError.class, testSubject::assertPublishedEvents);
    }

    @Test
    void assertPublishedEventsForEventMessages() {
        EventMessage eventMessage = asEventMessage(new StubDomainEvent());
        testSubject.handleSync(eventMessage, new LegacyMessageSupportingContext(eventMessage));

        testSubject.assertPublishedEvents(eventMessage);
    }
}
