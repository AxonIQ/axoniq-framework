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
package org.axonframework.messaging.core.timeout;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.annotation.AnnotatedMessageHandlingMemberDefinition;
import org.axonframework.messaging.core.annotation.ClasspathParameterResolverFactory;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static org.axonframework.messaging.core.annotation.MessageStreamResolverUtils.resolveToStream;
import static org.junit.jupiter.api.Assertions.*;

class TimeoutWrappedMessageHandlingMemberTest {

    private AnnotatedMessageHandlingMemberDefinition handlerDefinition;
    private ParameterResolverFactory parameterResolver;

    @BeforeEach
    void setUp() {
        parameterResolver = ClasspathParameterResolverFactory.forClass(getClass());
        handlerDefinition = new AnnotatedMessageHandlingMemberDefinition();
    }

    @Test
    void interruptsMessageHandlingMemberAsConfigured() throws NoSuchMethodException {
        Optional<MessageHandlingMember<TestMessageHandler>> optionalHandler = handlerDefinition.createHandler(
                TestMessageHandler.class,
                TestMessageHandler.class.getDeclaredMethod("handle", String.class),
                parameterResolver,
                result -> resolveToStream(result, new ClassBasedMessageTypeResolver())
        );
        assertTrue(optionalHandler.isPresent());
        MessageHandlingMember<TestMessageHandler> original = optionalHandler.get();

        TimeoutWrappedMessageHandlingMember<TestMessageHandler> wrappedHandler = new TimeoutWrappedMessageHandlingMember<>(
                original, 300, 200, 50
        );

        EventMessage eventMessage = EventTestUtils.asEventMessage("my-message");
        assertThrows(
                AxonTimeoutException.class,
                () -> wrappedHandler.handleSync(eventMessage,
                                                StubProcessingContext.forMessage(eventMessage),
                                                new TestMessageHandler())
        );
    }

    public static class TestMessageHandler {

        @EventHandler
        public void handle(String message) throws InterruptedException {
            Thread.sleep(500);
        }
    }
}