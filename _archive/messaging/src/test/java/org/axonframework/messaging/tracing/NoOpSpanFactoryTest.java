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

package org.axonframework.messaging.tracing;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The {@link NoOpSpanFactory} is not supposed to do anything, but should still provide basic requirements such as
 * returning a non-null span, and the span returning itself in certain situations.
 */
class NoOpSpanFactoryTest {

    private static final EventMessage TEST_EVENT =
            new GenericEventMessage(new MessageType("event"), "payload");

    @Test
    void createRootTraceReturnsNoOpSpan() {
        Span trace = NoOpSpanFactory.INSTANCE.createRootTrace(() -> "Trace");
        assertInstanceOf(NoOpSpanFactory.NoOpSpan.class, trace);
    }

    @Test
    void createHandlerSpanReturnsNoOpSpan() {
        Span trace = NoOpSpanFactory.INSTANCE.createHandlerSpan(() -> "Trace", TEST_EVENT, true);
        assertInstanceOf(NoOpSpanFactory.NoOpSpan.class, trace);
    }

    @Test
    void createDispatchSpanReturnsNoOpSpan() {
        Span trace = NoOpSpanFactory.INSTANCE.createDispatchSpan(() -> "Trace", TEST_EVENT);
        assertInstanceOf(NoOpSpanFactory.NoOpSpan.class, trace);
    }

    @Test
    void createInternalSpanWithMessageReturnsNoOpSpan() {
        Span trace = NoOpSpanFactory.INSTANCE.createInternalSpan(() -> "Trace", TEST_EVENT);
        assertInstanceOf(NoOpSpanFactory.NoOpSpan.class, trace);
    }

    @Test
    void createInternalSpanWithoutMessageReturnsNoOpSpan() {
        Span trace = NoOpSpanFactory.INSTANCE.createInternalSpan(() -> "Trace");
        assertInstanceOf(NoOpSpanFactory.NoOpSpan.class, trace);
    }

    @Test
    void propagateContextReturnsOriginal() {
        EventMessage message = TEST_EVENT;
        EventMessage result = NoOpSpanFactory.INSTANCE.propagateContext(message);
        assertSame(message, result);
    }

    @Test
    void noOpSpanReturnsSelfOnStart() {
        NoOpSpanFactory.NoOpSpan noOpSpan = new NoOpSpanFactory.NoOpSpan();
        assertSame(noOpSpan, noOpSpan.start());
    }

    @Test
    void noOpSpanReturnsSelfOnRecordException() {
        NoOpSpanFactory.NoOpSpan noOpSpan = new NoOpSpanFactory.NoOpSpan();
        assertSame(noOpSpan, noOpSpan.recordException(new RuntimeException("")));
    }
}
