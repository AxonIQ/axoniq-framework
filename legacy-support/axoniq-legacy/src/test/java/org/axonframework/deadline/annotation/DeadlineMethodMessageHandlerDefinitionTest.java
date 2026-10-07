/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package org.axonframework.deadline.annotation;

import org.axonframework.common.ObjectUtils;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.AnnotatedMessageHandlingMemberDefinition;
import org.axonframework.messaging.core.annotation.ClasspathParameterResolverFactory;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link DeadlineMethodMessageHandlerDefinition}, in particular that it wraps only
 * {@link DeadlineHandler @DeadlineHandler} methods, that the wrapped member only handles {@link DeadlineMessage}s
 * matching its configured deadline name, and that a name-specific handler outranks a wildcard one.
 */
class DeadlineMethodMessageHandlerDefinitionTest {

    private final AnnotatedMessageHandlingMemberDefinition handlerDefinition =
            new AnnotatedMessageHandlingMemberDefinition();
    private final ParameterResolverFactory parameterResolver =
            ClasspathParameterResolverFactory.forClass(getClass());
    private final DeadlineMethodMessageHandlerDefinition testSubject = new DeadlineMethodMessageHandlerDefinition();

    @Nested
    class WrapHandler {

        @Test
        void wrapsAMethodAnnotatedWithDeadlineHandler() throws NoSuchMethodException {
            // given
            MessageHandlingMember<Listener> handler = createHandler(Listener.class, "handleAnyDeadline", String.class);

            // when
            MessageHandlingMember<Listener> wrapped = testSubject.wrapHandler(handler);

            // then
            assertThat(wrapped).isNotSameAs(handler);
            assertThat(wrapped.unwrap(DeadlineHandlingMember.class)).isPresent();
        }

        @Test
        void leavesAPlainEventHandlerMethodUnwrapped() throws NoSuchMethodException {
            // given
            MessageHandlingMember<Listener> handler = createHandler(Listener.class, "handleEvent", String.class);

            // when
            MessageHandlingMember<Listener> wrapped = testSubject.wrapHandler(handler);

            // then
            assertThat(wrapped).isSameAs(handler);
        }
    }

    @Nested
    class CanHandle {

        @Test
        void aWildcardDeadlineHandlerAcceptsAnyDeadlineName() throws NoSuchMethodException {
            // given
            MessageHandlingMember<Listener> wrapped =
                    testSubject.wrapHandler(createHandler(Listener.class, "handleAnyDeadline", String.class));
            DeadlineMessage anyDeadline = new GenericDeadlineMessage("someDeadline", new MessageType("deadline"), "x");

            // when / then
            assertThat(wrapped.canHandle(anyDeadline, StubProcessingContext.forMessage(anyDeadline))).isTrue();
        }

        @Test
        void aNamedDeadlineHandlerOnlyAcceptsItsOwnDeadlineName() throws NoSuchMethodException {
            // given
            MessageHandlingMember<Listener> wrapped =
                    testSubject.wrapHandler(createHandler(Listener.class, "handleSpecificDeadline", String.class));
            DeadlineMessage matching = new GenericDeadlineMessage("specificDeadline", new MessageType("deadline"), "x");
            DeadlineMessage other = new GenericDeadlineMessage("someOtherDeadline", new MessageType("deadline"), "x");

            // when / then
            assertThat(wrapped.canHandle(matching, StubProcessingContext.forMessage(matching))).isTrue();
            assertThat(wrapped.canHandle(other, StubProcessingContext.forMessage(other))).isFalse();
        }

        @Test
        void aDeadlineHandlerNeverAcceptsAPlainEventMessage() throws NoSuchMethodException {
            // given
            MessageHandlingMember<Listener> wrapped =
                    testSubject.wrapHandler(createHandler(Listener.class, "handleAnyDeadline", String.class));
            GenericEventMessage notADeadline = new GenericEventMessage(new MessageType("event"), "x");

            // when / then
            assertThat(wrapped.canHandle(notADeadline, StubProcessingContext.forMessage(notADeadline))).isFalse();
        }
    }

    @Nested
    class Priority {

        @Test
        void aNamedDeadlineHandlerOutranksAWildcardOne() throws NoSuchMethodException {
            // given
            MessageHandlingMember<Listener> wildcard =
                    testSubject.wrapHandler(createHandler(Listener.class, "handleAnyDeadline", String.class));
            MessageHandlingMember<Listener> named =
                    testSubject.wrapHandler(createHandler(Listener.class, "handleSpecificDeadline", String.class));

            // when / then
            assertThat(named.priority()).isGreaterThan(wildcard.priority());
        }

        /**
         * A {@link DeadlineMessage} is an event message, so a plain {@link EventHandler} for the same payload type
         * could handle it too. The deadline handler has to be chosen instead.
         */
        @Test
        void aWildcardDeadlineHandlerOutranksAPlainEventHandlerForTheSamePayload() throws NoSuchMethodException {
            // given
            MessageHandlingMember<Listener> eventHandler =
                    testSubject.wrapHandler(createHandler(Listener.class, "handleEvent", String.class));
            MessageHandlingMember<Listener> deadlineHandler =
                    testSubject.wrapHandler(createHandler(Listener.class, "handleAnyDeadline", String.class));

            // when / then
            assertThat(deadlineHandler.priority()).isGreaterThan(eventHandler.priority());
        }
    }

    private static MessageStream<?> returnTypeConverter(Object result) {
        return MessageStream.just(new GenericMessage(new MessageType(ObjectUtils.nullSafeTypeOf(result)), result));
    }

    private <T> MessageHandlingMember<T> createHandler(Class<T> targetClass,
                                                        String methodName,
                                                        Class<?>... parameterTypes) throws NoSuchMethodException {
        return handlerDefinition.createHandler(
                                        targetClass,
                                        targetClass.getDeclaredMethod(methodName, parameterTypes),
                                        parameterResolver,
                                        DeadlineMethodMessageHandlerDefinitionTest::returnTypeConverter
                                )
                                .orElseThrow(() -> new IllegalArgumentException("Handler creation failed"));
    }

    @SuppressWarnings("unused")
    private static class Listener {

        @EventHandler
        public void handleEvent(String event) {
        }

        @DeadlineHandler
        public void handleAnyDeadline(String event) {
        }

        @DeadlineHandler(deadlineName = "specificDeadline")
        public void handleSpecificDeadline(String event) {
        }
    }
}
