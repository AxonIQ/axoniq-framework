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

package io.axoniq.framework.tracing.messaging;

import io.axoniq.framework.tracing.messaging.internal.TracingHandlerEnhancerDefinition;
import io.axoniq.framework.tracing.support.TestSpanFactory;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class TracingHandlerEnhancerDefinitionTest {

    private TestSpanFactory spanFactory;
    private TracingHandlerEnhancerDefinition testSubject;

    @BeforeEach
    void setUp() {
        spanFactory = new TestSpanFactory();
        testSubject = new TracingHandlerEnhancerDefinition(spanFactory);
    }

    @Nested
    class CommandHandlerEnhancement {

        @Test
        void wrapsCommandHandlerAndOpensASpanNamedAfterTheMethod() {
            // given
            StubHandlingMember<Object> member = new StubHandlingMember<>(true, handleMethod());
            CommandMessage command = new GenericCommandMessage(new MessageType("BookRoom"), "payload");

            // when
            MessageHandlingMember<Object> wrapped = testSubject.wrapHandler(member);
            wrapped.handle(command, new StubProcessingContext(), new BookRoomHandler());

            // then
            assertThat(wrapped).isNotSameAs(member);
            spanFactory.verifySpanActive("BookRoomHandler.handle(String)");
        }
    }

    @Nested
    class NonCommandHandlersAreNotEnhanced {

        @Test
        void leavesNonCommandHandlerUntouchedWithoutBuildingASpanName() {
            // given a member that does NOT handle commands; its signature builder would throw if ever invoked
            AtomicBoolean signatureBuilt = new AtomicBoolean(false);
            StubHandlingMember<Object> member = new StubHandlingMember<>(false, handleMethod());
            member.onUnwrapExecutable = () -> signatureBuilt.set(true);
            EventMessage event = EventTestUtils.asEventMessage("evt");

            // when
            MessageHandlingMember<Object> wrapped = testSubject.wrapHandler(member);
            wrapped.handle(event, new StubProcessingContext(), new BookRoomHandler());

            // then
            assertThat(wrapped).isSameAs(member);
            assertThat(signatureBuilt).isFalse();
            spanFactory.verifyNoSpan("BookRoomHandler.handle(String)");
        }
    }

    private static Method handleMethod() {
        try {
            return BookRoomHandler.class.getDeclaredMethod("handle", String.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unused")
    private static final class BookRoomHandler {

        void handle(String command) {
            // exercised only via the stub member
        }
    }

    /**
     * Minimal {@link MessageHandlingMember} stub. Reports whether it handles command messages, exposes a method as its
     * {@link Executable}, and returns an empty stream from {@code handle}.
     */
    private static final class StubHandlingMember<T> implements MessageHandlingMember<T> {

        private final boolean handlesCommands;
        private final Method method;
        private Runnable onUnwrapExecutable = () -> {
        };

        private StubHandlingMember(boolean handlesCommands, Method method) {
            this.handlesCommands = handlesCommands;
            this.method = method;
        }

        @Override
        public Class<?> payloadType() {
            return String.class;
        }

        @Override
        public boolean canHandle(Message message, ProcessingContext context) {
            return true;
        }

        @Override
        public boolean canHandleMessageType(Class<? extends Message> messageType) {
            return handlesCommands && CommandMessage.class.isAssignableFrom(messageType);
        }

        @Override
        public Object handleSync(Message message, ProcessingContext context, @Nullable T target) {
            return null;
        }

        @Override
        public MessageStream<?> handle(Message message, ProcessingContext context, @Nullable T target) {
            return MessageStream.empty();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <HT> Optional<HT> unwrap(Class<HT> handlerType) {
            if (handlerType.isAssignableFrom(Executable.class) || handlerType.equals(Executable.class)) {
                onUnwrapExecutable.run();
                return (Optional<HT>) Optional.of(method);
            }
            return Optional.empty();
        }
    }
}
