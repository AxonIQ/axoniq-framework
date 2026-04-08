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

package org.axonframework.messaging.core.annotation;

import org.axonframework.messaging.queryhandling.QueryMessage;
import org.junit.jupiter.api.*;

import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link WrappedMessageHandlingMember}.
 *
 * @author Steven van Beelen
 */
class WrappedMessageHandlingMemberTest {

    private MessageHandlingMember<Object> mockedHandlingMember;
    private WrappedMessageHandlingMember<Object> testSubject;

    @BeforeEach
    void setUp() {
        //noinspection unchecked
        mockedHandlingMember = mock(MessageHandlingMember.class);

        testSubject = new WrappedMessageHandlingMember<>(mockedHandlingMember) {
        };
    }

    @Test
    void canHandleMessageType() {
        testSubject.canHandleMessageType(QueryMessage.class);
        verify(mockedHandlingMember).canHandleMessageType(QueryMessage.class);
    }

    @Test
    void attribute() {
        testSubject.attribute(HandlerAttributes.COMMAND_ROUTING_KEY);
        verify(mockedHandlingMember).attribute(HandlerAttributes.COMMAND_ROUTING_KEY);
    }
}