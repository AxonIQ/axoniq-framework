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

import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static org.axonframework.messaging.core.annotation.MessageStreamResolverUtils.resolveToStream;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link MethodInvokingMessageHandlingMember}.
 *
 * @author Steven van Beelen
 */
class MethodInvokingMessageHandlingMemberTest {

    private MethodInvokingMessageHandlingMember<AnnotatedHandler> testSubject;

    @BeforeEach
    void setUp() {
        try {
            testSubject = new MethodInvokingMessageHandlingMember<>(
                    AnnotatedHandler.class.getMethod("handlingMethod", String.class),
                    EventMessage.class,
                    String.class,
                    ClasspathParameterResolverFactory.forClass(AnnotatedHandler.class),
                    result -> resolveToStream(result, new ClassBasedMessageTypeResolver())
            );
        } catch (NoSuchMethodException e) {
            fail(e.getMessage());
        }
    }

    @Test
    void canHandleMessageType() {
        assertTrue(testSubject.canHandleMessageType(EventMessage.class));
        assertFalse(testSubject.canHandleMessageType(CommandMessage.class));
    }

    @Test
    void attributeReturnsNonEmptyOptionalForMatchingAttributeKey() {
        Optional<Object> resultMessageType = testSubject.attribute(HandlerAttributes.MESSAGE_TYPE);
        Optional<Object> resultPayloadType = testSubject.attribute(HandlerAttributes.PAYLOAD_TYPE);

        assertTrue(resultMessageType.isPresent());
        assertEquals(EventMessage.class, resultMessageType.get());

        assertTrue(resultPayloadType.isPresent());
        assertEquals(Object.class, resultPayloadType.get());
    }

    @SuppressWarnings("unused")
    private static class AnnotatedHandler {

        @EventHandler
        public void handlingMethod(String event) {

        }
    }
}