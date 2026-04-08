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
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class MessageIdentifierParameterResolverFactoryTest {

    private MessageIdentifierParameterResolverFactory testSubject;

    private Method messageIdentifierMethod;
    private Method nonAnnotatedMethod;
    private Method integerMethod;

    @BeforeEach
    void setUp() throws Exception {
        testSubject = new MessageIdentifierParameterResolverFactory();

        messageIdentifierMethod = getClass().getMethod("someMessageIdentifierMethod", String.class);
        nonAnnotatedMethod = getClass().getMethod("someNonAnnotatedMethod", String.class);
        integerMethod = getClass().getMethod("someIntegerMethod", Integer.class);
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    public void someMessageIdentifierMethod(@MessageIdentifier String messageIdentifier) {
        //Used in setUp()
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    public void someNonAnnotatedMethod(String messageIdentifier) {
        //Used in setUp()
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    public void someIntegerMethod(@MessageIdentifier Integer messageIdentifier) {
        //Used in setUp()
    }

    @Test
    void resolvesToMessageIdentifierWhenAnnotatedForEventMessage() {
        ParameterResolver<String> resolver =
                testSubject.createInstance(messageIdentifierMethod, messageIdentifierMethod.getParameters(), 0);
        assertNotNull(resolver);
        final EventMessage eventMessage = EventTestUtils.asEventMessage("test");
        ProcessingContext context = StubProcessingContext.forMessage(eventMessage);
        assertTrue(resolver.matches(context));
        assertEquals(eventMessage.identifier(), resolver.resolveParameterValue(context).join());
    }

    @Test
    void resolvesToMessageIdentifierWhenAnnotatedForCommandMessage() {
        ParameterResolver<String> resolver =
                testSubject.createInstance(messageIdentifierMethod, messageIdentifierMethod.getParameters(), 0);
        assertNotNull(resolver);
        CommandMessage commandMessage =
                new GenericCommandMessage(new MessageType("command"), "test");
        ProcessingContext context = StubProcessingContext.forMessage(commandMessage);
        assertTrue(resolver.matches(context));
        assertEquals(commandMessage.identifier(), resolver.resolveParameterValue(context).join());
    }

    @Test
    void ignoredWhenNotAnnotated() {
        ParameterResolver<String> resolver =
                testSubject.createInstance(nonAnnotatedMethod, nonAnnotatedMethod.getParameters(), 0);
        assertNull(resolver);
    }

    @Test
    void ignoredWhenWrongType() {
        ParameterResolver<String> resolver =
                testSubject.createInstance(integerMethod, integerMethod.getParameters(), 0);
        assertNull(resolver);
    }
}
