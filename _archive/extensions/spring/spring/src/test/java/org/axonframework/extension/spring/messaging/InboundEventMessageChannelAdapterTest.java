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

package org.axonframework.extension.spring.messaging;

import org.axonframework.messaging.eventhandling.EventBus;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.extension.spring.util.StubDomainEvent;
import org.junit.jupiter.api.*;
import org.mockito.*;
import org.springframework.messaging.support.GenericMessage;

import static org.mockito.Mockito.*;

/**
 * @author Allard Buijze
 */
class InboundEventMessageChannelAdapterTest {

    private EventBus mockEventBus;
    private InboundEventMessageChannelAdapter testSubject;

    @BeforeEach
    void setUp() {
        mockEventBus = mock(EventBus.class);
        testSubject = new InboundEventMessageChannelAdapter(mockEventBus);
    }

    @Test
    void messagePayloadIsPublished() {
        testSubject = new InboundEventMessageChannelAdapter();
        StubDomainEvent event = new StubDomainEvent();
        testSubject.handleMessage(new GenericMessage(event));

        verify(mockEventBus, never()).publish(eq(null), isA(EventMessage.class));

        testSubject.subscribe((events, context) -> mockEventBus.publish(context, events.stream().map(it -> (EventMessage) it).toList()));

        testSubject.handleMessage(new GenericMessage(event));

        verify(mockEventBus).publish(eq(null), ArgumentMatchers.anyList());
    }

}
