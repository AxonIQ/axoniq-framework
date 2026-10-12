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

package io.axoniq.framework.axonserver.connector.command;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.framework.axonserver.connector.api.command.AxonServerRemoteCommandHandlingException;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the Axon Server wire format of commands and command results to golden samples produced by the real code of
 * released Axon Server connectors, so that nodes of different versions in one cluster keep understanding each other.
 * <p>
 * See the {@code README.md} next to the golden files for how they were produced.
 *
 * @author Mateusz Nowak
 */
class CommandConverterWireCompatibilityTest {

    private static final String GOLDEN_LOCATION = "/io/axoniq/framework/axonserver/connector/command/wire/";
    private static final MessageType DISPATCHED_COMMAND_TYPE = new MessageType("ValueCommand");
    private static final MessageType VOID_RESULT_TYPE = new MessageType("java.lang.Void");
    private static final MessageType VALUE_RESULT_TYPE = new MessageType("WireGoldenGenerator$ValueResult", "2.0");
    private static final byte[] VALUE_RESULT_DATA = "{\"value\":\"some-value\"}".getBytes(StandardCharsets.UTF_8);
    private static final String CLIENT_ID = "golden-client";
    private static final String COMPONENT_NAME = "golden-component";

    private final Converter converter = new JacksonConverter();

    @Nested
    class ReadingResponsesOfReleasedHandlers {

        @ParameterizedTest
        @ValueSource(strings = {"5.3.3", "5.4.0"})
        void voidResultIsReadAsNullPayloadKeepingVoidTypeIdentifierAndMetadata(String version) {
            // given
            CommandResponse golden = response(version, "void-result");

            // when
            CommandResultMessage result = read(golden);

            // then
            assertThat(result.payload()).isNull();
            assertThat(result.<Void>payloadAs(Void.class)).isNull();
            assertThat(result.type()).isEqualTo(VOID_RESULT_TYPE);
            assertThat(result.identifier()).isEqualTo(golden.getMessageIdentifier());
            assertThat(result.metadata()).isEmpty();
        }

        @ParameterizedTest
        @ValueSource(strings = {"5.3.3", "5.4.0"})
        void valueResultKeepsTypeRevisionDataIdentifierAndMetadata(String version) {
            // given
            CommandResponse golden = response(version, "value-result");

            // when
            CommandResultMessage result = read(golden);

            // then
            assertThat(result.type()).isEqualTo(VALUE_RESULT_TYPE);
            assertThat(result.payload()).isEqualTo(VALUE_RESULT_DATA);
            assertThat(result.identifier()).isEqualTo(golden.getMessageIdentifier());
            assertThat(result.metadata()).containsExactly(Map.entry("result-key", "result-value"));
        }

        @ParameterizedTest
        @ValueSource(strings = {"5.3.3", "5.4.0"})
        void emptyStringResultStaysAnEmptyString(String version) {
            // given
            CommandResponse golden = response(version, "empty-string-result");

            // when
            CommandResultMessage result = read(golden);

            // then
            assertThat(result.type()).isEqualTo(new MessageType(String.class));
            assertThat(result.payload()).isEqualTo(new byte[0]);
            assertThat(result.<String>payloadAs(String.class)).isEmpty();
        }

        @ParameterizedTest
        @ValueSource(strings = {"5.3.3", "5.4.0"})
        void emptyByteArrayResultStaysAnEmptyByteArray(String version) {
            // given
            CommandResponse golden = response(version, "empty-bytes-result");

            // when
            CommandResultMessage result = read(golden);

            // then
            assertThat(result.type()).isEqualTo(new MessageType(byte[].class));
            assertThat(result.<byte[]>payloadAs(byte[].class)).isEmpty();
        }

        @ParameterizedTest
        @ValueSource(strings = {"5.3.3", "5.4.0"})
        void errorResultFailsWithCommandExecutionExceptionCausedByRemoteHandlingException(String version) {
            // given
            CommandResponse golden = response(version, "error-result");

            // when
            var future = CommandConverter.convertCommandResponse(golden, converter, DISPATCHED_COMMAND_TYPE);

            // then the same exception the released dispatchers raise
            assertThat(future).failsWithin(Duration.ofSeconds(1));
            assertThat(future.exceptionNow())
                    .isExactlyInstanceOf(CommandExecutionException.class)
                    .hasMessage("java.lang.IllegalStateException: Handler failed on purpose")
                    .cause()
                    .isExactlyInstanceOf(AxonServerRemoteCommandHandlingException.class)
                    .hasMessageContaining("Handler failed on purpose");
        }

        private CommandResultMessage read(CommandResponse golden) {
            return CommandConverter.convertCommandResponse(golden, converter, DISPATCHED_COMMAND_TYPE)
                                   .orTimeout(1, TimeUnit.SECONDS)
                                   .join();
        }
    }

    @Nested
    class RespondingAsReleasedHandlers {

        @ParameterizedTest
        @ValueSource(strings = {"5.3.3", "5.4.0"})
        void voidResultMatchesReleasedResponseApartFromMessageIdentifier(String version) {
            // given the result the command bus hands to the connector for a handler that returns nothing
            CommandResponse golden = response(version, "void-result");
            var resultMessage = new GenericCommandResultMessage(new MessageType(Void.class), (Object) null);

            // when
            CommandResponse response =
                    CommandConverter.convertResultMessage(resultMessage, golden.getRequestIdentifier());

            // then released dispatchers receive the exact same bytes as from a released handler
            assertThat(response.hasPayload()).isTrue();
            assertThat(withoutMessageIdentifier(response).toByteArray())
                    .isEqualTo(withoutMessageIdentifier(golden).toByteArray());
        }

        @ParameterizedTest
        @ValueSource(strings = {"5.3.3", "5.4.0"})
        void valueResultMatchesReleasedResponse(String version) {
            // given
            CommandResponse golden = response(version, "value-result");
            var resultMessage = new GenericCommandResultMessage(new GenericMessage(
                    golden.getMessageIdentifier(),
                    VALUE_RESULT_TYPE,
                    VALUE_RESULT_DATA,
                    Map.of("result-key", "result-value")
            ));

            // when
            CommandResponse response =
                    CommandConverter.convertResultMessage(resultMessage, golden.getRequestIdentifier());

            // then
            assertThat(response.toByteArray()).isEqualTo(golden.toByteArray());
        }

        @ParameterizedTest
        @ValueSource(strings = {"5.3.3", "5.4.0"})
        void emptyStringResultMatchesReleasedResponseApartFromMessageIdentifier(String version) {
            // given
            CommandResponse golden = response(version, "empty-string-result");
            var resultMessage = new GenericCommandResultMessage(new MessageType(String.class), new byte[0]);

            // when
            CommandResponse response =
                    CommandConverter.convertResultMessage(resultMessage, golden.getRequestIdentifier());

            // then
            assertThat(withoutMessageIdentifier(response).toByteArray())
                    .isEqualTo(withoutMessageIdentifier(golden).toByteArray());
        }

        @ParameterizedTest
        @ValueSource(strings = {"5.3.3", "5.4.0"})
        void emptyByteArrayResultMatchesReleasedResponseApartFromMessageIdentifier(String version) {
            // given
            CommandResponse golden = response(version, "empty-bytes-result");
            var resultMessage = new GenericCommandResultMessage(new MessageType(byte[].class), new byte[0]);

            // when
            CommandResponse response =
                    CommandConverter.convertResultMessage(resultMessage, golden.getRequestIdentifier());

            // then
            assertThat(withoutMessageIdentifier(response).toByteArray())
                    .isEqualTo(withoutMessageIdentifier(golden).toByteArray());
        }

        @ParameterizedTest
        @ValueSource(strings = {"5.3.3", "5.4.0"})
        void errorResultMatchesReleasedResponseApartFromMessageIdentifier(String version) {
            // given the handler's exception, which reaches the connector wrapped as the command bus completes the
            // result future exceptionally
            CommandResponse golden = response(version, "error-result");
            var cause = new CompletionException(new IllegalStateException("Handler failed on purpose"));

            // when
            CommandResponse response = CommandConverter.convertErrorResponse(
                    CLIENT_ID, golden.getRequestIdentifier(), cause, converter
            );

            // then
            assertThat(withoutMessageIdentifier(response).toByteArray())
                    .isEqualTo(withoutMessageIdentifier(golden).toByteArray());
        }

        private CommandResponse withoutMessageIdentifier(CommandResponse response) {
            return response.toBuilder().clearMessageIdentifier().build();
        }
    }

    @Nested
    class ExchangingCommandsWithReleasedNodes {

        @ParameterizedTest
        @ValueSource(strings = {"5.3.3", "5.4.0"})
        void dispatchedCommandMatchesReleasedCommand(String version) {
            // given
            Command golden = command(version);
            var commandMessage = new GenericCommandMessage(
                    new GenericMessage(golden.getMessageIdentifier(),
                                       DISPATCHED_COMMAND_TYPE,
                                       "{}".getBytes(StandardCharsets.UTF_8),
                                       Map.of("command-key", "command-value")),
                    "routing-key",
                    3
            );

            // when
            Command command = CommandConverter.convertCommandMessage(commandMessage, CLIENT_ID, COMPONENT_NAME);

            // then
            assertThat(command.toByteArray()).isEqualTo(golden.toByteArray());
        }

        @ParameterizedTest
        @ValueSource(strings = {"5.3.3", "5.4.0"})
        void commandOfReleasedDispatcherIsReadIntact(String version) {
            // given
            Command golden = command(version);

            // when
            var commandMessage = CommandConverter.convertCommand(golden, converter);

            // then
            assertThat(commandMessage.identifier()).isEqualTo("value-result-command-id");
            assertThat(commandMessage.type()).isEqualTo(DISPATCHED_COMMAND_TYPE);
            assertThat(commandMessage.payload()).isEqualTo("{}".getBytes(StandardCharsets.UTF_8));
            assertThat(commandMessage.metadata()).containsExactly(Map.entry("command-key", "command-value"));
            assertThat(commandMessage.routingKey()).hasValue("routing-key");
            assertThat(commandMessage.priority()).hasValue(3);
        }
    }

    private static CommandResponse response(String version, String sample) {
        try {
            return CommandResponse.parseFrom(golden("axoniq-" + version + "-" + sample + ".bin"));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Golden response is not a CommandResponse", e);
        }
    }

    private static Command command(String version) {
        try {
            return Command.parseFrom(golden("axoniq-" + version + "-command.bin"));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Golden command is not a Command", e);
        }
    }

    private static byte[] golden(String fileName) {
        try (InputStream golden = CommandConverterWireCompatibilityTest.class.getResourceAsStream(
                GOLDEN_LOCATION + fileName
        )) {
            assertThat(golden).as("golden file %s", fileName).isNotNull();
            return golden.readAllBytes();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read golden file " + fileName, e);
        }
    }
}
