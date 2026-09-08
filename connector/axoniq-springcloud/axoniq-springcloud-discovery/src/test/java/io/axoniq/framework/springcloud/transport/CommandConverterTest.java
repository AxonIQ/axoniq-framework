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

import org.axonframework.common.AxonNonTransientException;
import org.axonframework.conversion.ConversionException;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.NoHandlerForCommandException;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.RemoteHandlingException;
import org.axonframework.messaging.core.RemoteNonTransientHandlingException;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the wire format {@link CommandConverter} produces, in both directions.
 *
 * @author Allard Buijze
 */
class CommandConverterTest {

    private static final MessageType COMMAND_TYPE = new MessageType("university.CreateCourse", "2.1.0");
    private static final MessageType RESULT_TYPE = new MessageType("university.CourseId", "1.0.0");
    private static final String PAYLOAD = "{\"name\":\"Axon 5\"}";

    private static CommandMessage command() {
        return new GenericCommandMessage(
                new GenericMessage("command-1", COMMAND_TYPE, PAYLOAD, Map.of("tenant", "acme")),
                "course-42",
                7
        );
    }

    @Nested
    class DispatchingACommand {

        @Test
        void carriesEveryFieldTheReceivingMemberNeeds() {
            // when
            CommandDispatchRequest request = CommandConverter.convertCommandMessage(command());

            // then
            assertThat(request.identifier()).isEqualTo("command-1");
            assertThat(request.type()).isEqualTo("university.CreateCourse#2.1.0");
            assertThat(request.metadata()).containsEntry("tenant", "acme");
            assertThat(request.routingKey()).isEqualTo("course-42");
            assertThat(request.priority()).isEqualTo(7);
        }

        @Test
        void roundTripsACommand() {
            // given
            CommandMessage original = command();

            // when
            CommandMessage roundTripped = CommandConverter.convertRequest(
                    CommandConverter.convertCommandMessage(original), null
            );

            // then
            assertThat(roundTripped.identifier()).isEqualTo(original.identifier());
            assertThat(roundTripped.type()).isEqualTo(original.type());
            assertThat(roundTripped.payload()).isEqualTo(PAYLOAD);
            assertThat(roundTripped.metadata()).isEqualTo(original.metadata());
            assertThat(roundTripped.routingKey()).contains("course-42");
            assertThat(roundTripped.priority()).hasValue(7);
        }

        @Test
        void roundTripsACommandWithoutRoutingKeyOrPriority() {
            // given
            CommandMessage original = new GenericCommandMessage(
                    new GenericMessage("command-2", COMMAND_TYPE, PAYLOAD, Map.of())
            );

            // when
            CommandMessage roundTripped = CommandConverter.convertRequest(
                    CommandConverter.convertCommandMessage(original), null
            );

            // then
            assertThat(roundTripped.routingKey()).isEmpty();
            assertThat(roundTripped.priority()).isEmpty();
        }

        @Test
        void rejectsAPayloadThatWasNeverConverted() {
            // given — a connector not wrapped in a PayloadConvertingCommandBusConnector would produce this: a
            // payload still in its domain form, with no converter to write it as text
            CommandMessage unconverted = new GenericCommandMessage(COMMAND_TYPE, Map.of("name", "Axon 5"));

            // when / then
            assertThatThrownBy(() -> CommandConverter.convertCommandMessage(unconverted))
                    .isInstanceOf(ConversionException.class)
                    .hasMessageContaining("java.lang.String");
        }
    }

    @Nested
    class ReplyingWithAResult {

        @Test
        void roundTripsAResult() {
            // given
            CommandResultMessage result = new GenericCommandResultMessage(
                    new GenericMessage("result-1", RESULT_TYPE, PAYLOAD, Map.of("trace", "abc"))
            );

            // when
            CommandResultMessage roundTripped = CommandConverter.convertReply(
                    CommandConverter.convertResultMessage(result, "command-1"), null
            );

            // then
            assertThat(roundTripped).isNotNull();
            assertThat(roundTripped.identifier()).isEqualTo("result-1");
            assertThat(roundTripped.type()).isEqualTo(RESULT_TYPE);
            assertThat(roundTripped.payload()).isEqualTo(PAYLOAD);
            assertThat(roundTripped.metadata()).containsEntry("trace", "abc");
        }

        @Test
        void correlatesTheReplyToTheCommand() {
            // given
            CommandResultMessage result = new GenericCommandResultMessage(RESULT_TYPE, PAYLOAD);

            // when
            CommandDispatchReply reply = CommandConverter.convertResultMessage(result, "command-1");

            // then
            assertThat(reply.requestIdentifier()).isEqualTo("command-1");
            assertThat(reply.isError()).isFalse();
        }

        @Test
        void reportsAHandlerThatReturnedNothing() {
            // when
            CommandDispatchReply reply = CommandConverter.convertResultMessage(null, "command-1");

            // then — absent result and absent failure must be distinguishable
            assertThat(reply.type()).isNull();
            assertThat(reply.isError()).isFalse();
            assertThat(CommandConverter.convertReply(reply, null)).isNull();
        }

        @Test
        void reportsAResultWithoutAPayload() {
            // given
            CommandResultMessage result = new GenericCommandResultMessage(
                    new GenericMessage("result-1", RESULT_TYPE, null, Map.of())
            );

            // when
            CommandDispatchReply reply = CommandConverter.convertResultMessage(result, "command-1");

            // then — a result that exists but carries nothing keeps its type, so it is not mistaken for no result
            assertThat(reply.type()).isEqualTo(RESULT_TYPE.toString());
            assertThat(reply.payload()).isNull();
            assertThat(CommandConverter.convertReply(reply, null)).isNotNull();
        }
    }

    @Nested
    class ReplyingWithAFailure {

        @Test
        void reconstructsAMissingHandlerAsATransientFailure() {
            // given
            CommandDispatchReply reply = CommandConverter.convertErrorResult(
                    new NoHandlerForCommandException("nothing handles this here"), "command-1", "node-b", null
            );

            // when / then
            assertThat(reply.errorCode()).isEqualTo(CommandErrorCode.NO_HANDLER_FOR_COMMAND);
            assertThatThrownBy(() -> CommandConverter.convertReply(reply, null))
                    .isInstanceOf(NoHandlerForCommandException.class)
                    .isNotInstanceOf(AxonNonTransientException.class)
                    .hasMessageContaining("nothing handles this here");
        }

        @Test
        void reconstructsAFailedHandlerAsARetryableExecutionFailure() {
            // given
            CommandDispatchReply reply = CommandConverter.convertErrorResult(
                    new IllegalStateException("course is full"), "command-1", "node-b", null
            );

            // when / then
            assertThat(reply.errorCode()).isEqualTo(CommandErrorCode.COMMAND_EXECUTION_ERROR);
            assertThatThrownBy(() -> CommandConverter.convertReply(reply, null))
                    .isInstanceOf(CommandExecutionException.class)
                    .hasMessageContaining("course is full")
                    .hasCauseInstanceOf(RemoteHandlingException.class);
        }

        @Test
        void preservesThatAFailureWasNonTransient() {
            // given — a handler failure the application marked as not worth retrying
            CommandDispatchReply reply = CommandConverter.convertErrorResult(
                    new NonRetryableFailure("course does not exist"), "command-1", "node-b", null
            );

            // when / then — losing this across the wire would have a RetryScheduler retry a hopeless command
            assertThat(reply.errorCode()).isEqualTo(CommandErrorCode.COMMAND_EXECUTION_NON_TRANSIENT_ERROR);
            assertThatThrownBy(() -> CommandConverter.convertReply(reply, null))
                    .isInstanceOf(CommandExecutionException.class)
                    .hasCauseInstanceOf(RemoteNonTransientHandlingException.class);
        }

        @Test
        void namesTheMemberTheFailureCameFrom() {
            // given
            CommandDispatchReply reply = CommandConverter.convertErrorResult(
                    new IllegalStateException("course is full"), "command-1", "node-b", null
            );

            // when / then
            assertThat(reply.errorOrigin()).isEqualTo("node-b");
            assertThatThrownBy(() -> CommandConverter.convertReply(reply, null))
                    .hasMessageContaining("node-b");
        }

        @Test
        void carriesTheCauseChainForDiagnostics() {
            // given
            Throwable cause = new IllegalStateException("course is full",
                                                       new IllegalArgumentException("capacity reached"));

            // when
            CommandDispatchReply reply = CommandConverter.convertErrorResult(cause, "command-1", "node-b", null);

            // then
            assertThat(reply.errorDetails()).containsExactly("course is full", "capacity reached");
        }

        @Test
        void describesAFailureThatCarriesNoMessage() {
            // given
            CommandDispatchReply reply = CommandConverter.convertErrorResult(
                    new IllegalStateException(), "command-1", "node-b", null
            );

            // when / then — the class name is more use than an empty message
            assertThat(reply.errorMessage()).isEqualTo(IllegalStateException.class.getName());
        }

        @Test
        void treatsAReplyWithoutAnErrorCodeAsAnAbsentResultRatherThanAFailure() {
            // given — a reply carrying an error message but no code, as a member of an older shape might send
            CommandDispatchReply reply = new CommandDispatchReply(
                    "reply-1", "command-1", null, null, Map.of(),
                    null, "something went wrong", List.of(), "node-b", null, null
            );

            // when / then — the error code is what marks a reply as a failure, so this reads as no result at all
            assertThat(reply.isError()).isFalse();
            assertThat(CommandConverter.convertReply(reply, null)).isNull();
        }

        @Test
        void carriesApplicationDetailsAlreadyInBytes() {
            // given
            byte[] details = "{\"code\":42}".getBytes(StandardCharsets.UTF_8);
            CommandExecutionException withDetails =
                    new CommandExecutionException("course is full", new IllegalStateException(), details);

            // when
            CommandDispatchReply reply = CommandConverter.convertErrorResult(
                    withDetails, "command-1", "node-b", null
            );

            // then
            assertThat(reply.errorDetailsPayload()).isNotNull();
            assertThatThrownBy(() -> CommandConverter.convertReply(reply, null))
                    .isInstanceOf(CommandExecutionException.class)
                    .satisfies(thrown -> assertThat(
                            ((CommandExecutionException) thrown).<byte[]>getDetails()
                    ).contains(details));
        }

        @Test
        void omitsApplicationDetailsThatCannotBeConverted() {
            // given — details that are not bytes, with no converter available to serialize them
            CommandExecutionException withDetails = new CommandExecutionException(
                    "course is full", new IllegalStateException(), new Object()
            );

            // when
            CommandDispatchReply reply = CommandConverter.convertErrorResult(
                    withDetails, "command-1", "node-b", null
            );

            // then — the failure itself still crosses the wire; only the details are dropped
            assertThat(reply.errorDetailsPayload()).isNull();
            assertThat(reply.errorCode()).isEqualTo(CommandErrorCode.COMMAND_EXECUTION_ERROR);
        }
    }

    /**
     * A handler failure the application declared not worth retrying.
     */
    private static class NonRetryableFailure extends AxonNonTransientException {

        private NonRetryableFailure(String message) {
            super(message);
        }
    }
}
