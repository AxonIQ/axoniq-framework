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

package io.axoniq.framework.springcloud.transport;

import io.axoniq.framework.springcloud.util.RecordingCommandHandler;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.NoHandlerForCommandException;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests how {@link IncomingCommandGateway} turns a request from another member into a local handler invocation and
 * back into a reply.
 *
 * @author Allard Buijze
 */
class IncomingCommandGatewayTest {

    private static final MessageType COMMAND_TYPE = new MessageType("university.CreateCourse", "2.1.0");
    private static final MessageType RESULT_TYPE = new MessageType("university.CourseId", "1.0.0");
    private static final byte[] PAYLOAD = "{\"name\":\"Axon 5\"}".getBytes(StandardCharsets.UTF_8);

    private RecordingCommandHandler handler;
    private IncomingCommandGateway testSubject;

    @BeforeEach
    void setUp() {
        handler = new RecordingCommandHandler();
        testSubject = new IncomingCommandGateway(() -> "node-b", null);
    }

    private static CommandDispatchRequest request() {
        return CommandConverter.convertCommandMessage(new GenericCommandMessage(
                new GenericMessage("command-1", COMMAND_TYPE, PAYLOAD, Map.of("tenant", "acme")),
                "course-42",
                7
        ));
    }

    @Nested
    class WithAHandlerBound {

        @BeforeEach
        void bindHandler() {
            testSubject.bind(handler);
        }

        @Test
        void handsTheCommandToTheBoundHandler() {
            // when
            testSubject.handle(request()).join();

            // then
            assertThat(handler.handled()).hasSize(1);
            CommandMessage handled = handler.lastHandled();
            assertThat(handled.identifier()).isEqualTo("command-1");
            assertThat(handled.type()).isEqualTo(COMMAND_TYPE);
            assertThat(handled.routingKey()).contains("course-42");
            assertThat(handled.priority()).hasValue(7);
            assertThat(handled.metadata()).containsEntry("tenant", "acme");
        }

        @Test
        void repliesWithTheHandlerResult() {
            // given
            handler.answeringWith(new GenericCommandResultMessage(
                    new GenericMessage("result-1", RESULT_TYPE, PAYLOAD, Map.of())
            ));

            // when
            CommandDispatchReply reply = testSubject.handle(request()).join();

            // then
            assertThat(reply.isError()).isFalse();
            assertThat(reply.requestIdentifier()).isEqualTo("command-1");
            assertThat(CommandConverter.convertReply(reply, null)).isNotNull();
        }

        @Test
        void repliesWithoutAResultWhenTheHandlerReturnedNone() {
            // when
            CommandDispatchReply reply = testSubject.handle(request()).join();

            // then
            assertThat(reply.isError()).isFalse();
            assertThat(reply.type()).isNull();
        }

        @Test
        void repliesWithTheFailureWhenTheHandlerThrows() {
            // given
            handler.failingWith(new IllegalStateException("course is full"));

            // when
            CompletableFuture<CommandDispatchReply> result = testSubject.handle(request());

            // then — a handler failure is reported in the reply, so the future itself succeeds
            assertThat(result).isCompleted();
            CommandDispatchReply reply = result.join();
            assertThat(reply.errorCode()).isEqualTo(CommandErrorCode.COMMAND_EXECUTION_ERROR);
            assertThat(reply.errorMessage()).contains("course is full");
            assertThat(reply.errorOrigin()).isEqualTo("node-b");
        }

        @Test
        void repliesWithAFailureWhenTheRequestCannotBeRead() {
            // given — a request whose type is not a valid MessageType string
            CommandDispatchRequest malformed = new CommandDispatchRequest(
                    "command-1", "not-a-message-type", null, Map.of(), null, null
            );

            // when
            CommandDispatchReply reply = testSubject.handle(malformed).join();

            // then — the command never reached a handler, so it is a dispatch failure rather than an execution one
            assertThat(reply.errorCode()).isEqualTo(CommandErrorCode.COMMAND_DISPATCH_ERROR);
            assertThat(handler.handled()).isEmpty();
        }
    }

    @Nested
    class WithoutAHandlerBound {

        @Test
        void repliesThatNoHandlerIsAvailableYet() {
            // when — a command can arrive before DistributedCommandBus has registered its handler
            CommandDispatchReply reply = testSubject.handle(request()).join();

            // then
            assertThat(reply.errorCode()).isEqualTo(CommandErrorCode.NO_HANDLER_FOR_COMMAND);
            assertThat(reply.errorMessage()).contains("still starting up");
        }

        @Test
        void letsTheDispatchingMemberSeeATransientFailure() {
            // given
            CommandDispatchReply reply = testSubject.handle(request()).join();

            // when / then — a transient failure is what lets the dispatching member retry once this one is up
            assertThatThrownBy(() -> CommandConverter.convertReply(reply, null))
                    .isInstanceOf(NoHandlerForCommandException.class);
        }

        @Test
        void startsHandlingOnceAHandlerIsBound() {
            // given
            testSubject.handle(request()).join();

            // when
            testSubject.bind(handler);
            CommandDispatchReply reply = testSubject.handle(request()).join();

            // then
            assertThat(reply.isError()).isFalse();
            assertThat(handler.handled()).hasSize(1);
        }
    }

    @Nested
    class Binding {

        @Test
        void replacesAPreviouslyBoundHandler() {
            // given
            RecordingCommandHandler replaced = new RecordingCommandHandler();
            testSubject.bind(replaced);

            // when
            testSubject.bind(handler);
            testSubject.handle(request()).join();

            // then
            assertThat(handler.handled()).hasSize(1);
            assertThat(replaced.handled()).isEmpty();
        }

        @Test
        void rejectsANullHandler() {
            // when / then
            assertThatThrownBy(() -> testSubject.bind(null)).isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsANullMemberName() {
            // when / then
            assertThatThrownBy(() -> new IncomingCommandGateway(null, null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsANullRequest() {
            // when / then
            assertThatThrownBy(() -> testSubject.handle(null)).isInstanceOf(NullPointerException.class);
        }
    }
}
