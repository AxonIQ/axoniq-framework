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

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.junit.jupiter.*;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.grpc.ProcessingKey.PRIORITY;
import static io.axoniq.axonserver.grpc.ProcessingKey.ROUTING_KEY;
import static io.axoniq.framework.axonserver.connector.util.ProcessingInstructionUtils.createProcessingInstruction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CommandConverterTest {

    private final String messageIdentifier = UUID.randomUUID().toString();
    private final String stringPayload = "payload";
    private final byte[] payload = stringPayload.getBytes();
    private final String clientId = "clientId";
    private final String componentName = "componentName";
    private Converter converter;

    @BeforeEach
    void setUp() {
        converter = spy(new JacksonConverter());
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(converter);
    }

    @Nested
    class ConvertCommandMessage {

        @Test
        void convertsCommandMessageToCommand() {
            // given
            var type = new MessageType("CommandType", "1");
            var command = new GenericCommandMessage(
                    new GenericMessage(messageIdentifier, type, payload, Map.of("k", "v")),
                    "routingKey",
                    5
            );

            // when
            var grpcCommand = CommandConverter.convertCommandMessage(command, clientId, componentName);

            // then
            assertThat(grpcCommand.getClientId()).isEqualTo(clientId);
            assertThat(grpcCommand.getComponentName()).isEqualTo(componentName);
            assertThat(grpcCommand.getMessageIdentifier()).isEqualTo(messageIdentifier);
            assertThat(grpcCommand.getName()).isEqualTo("CommandType");
            assertThat(grpcCommand.getPayload().getType()).isEqualTo("CommandType");
            assertThat(grpcCommand.getPayload().getRevision()).isEqualTo("1");
            assertThat(grpcCommand.getPayload().getData().toByteArray()).isEqualTo(payload);
            assertThat(grpcCommand.getMetaDataMap().get("k").getTextValue()).isEqualTo("v");
            assertThat(grpcCommand.getProcessingInstructionsList())
                    .anySatisfy(pi -> {
                        assertThat(pi.getKey()).isEqualTo(ROUTING_KEY);
                        assertThat(pi.getValue().getTextValue()).isEqualTo("routingKey");
                    })
                    .anySatisfy(pi -> {
                        assertThat(pi.getKey()).isEqualTo(PRIORITY);
                        assertThat(pi.getValue().getNumberValue()).isEqualTo(5L);
                    });
        }

        @Test
        void convertsCommandMessageWithoutRoutingKeyAndPriorityToCommandWithoutProcessingInstructions() {
            // given
            var command = new GenericCommandMessage(
                    new GenericMessage(messageIdentifier, new MessageType("CommandType", "1"), payload, Map.of())
            );

            // when
            var grpcCommand = CommandConverter.convertCommandMessage(command, clientId, componentName);

            // then
            assertThat(grpcCommand.getProcessingInstructionsList()).isEmpty();
        }

        @Test
        void convertCommandMessageThrowsOnNonByteArrayPayload() {
            // given
            var command = new GenericCommandMessage(new MessageType("CommandType", "1"), "not-bytes");

            // when / then
            assertThatThrownBy(() -> CommandConverter.convertCommandMessage(command, clientId, componentName))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Payload must be of type byte[]");
        }
    }

    @Nested
    class ConvertCommandResponse {

        @Test
        void convertsCommandResponseToCommandResultMessage() {
            // given
            String expectedPayload = "ok";
            var response = CommandResponse.newBuilder()
                                          .setMessageIdentifier(messageIdentifier)
                                          .setPayload(SerializedObject.newBuilder()
                                                                      .setType("java.lang.String")
                                                                      .setRevision("1")
                                                                      .setData(ByteString.copyFrom(expectedPayload.getBytes()))
                                                                      .build())
                                          .putMetaData("m", MetaDataValue.newBuilder().setTextValue("v").build())
                                          .build();

            // when
            var future = CommandConverter.convertCommandResponse(response, converter);

            // then
            var resultMessage = future.orTimeout(1, TimeUnit.SECONDS).join();
            assertThat(resultMessage.identifier()).isEqualTo(messageIdentifier);
            assertThat(resultMessage.type().name()).isEqualTo("java.lang.String");
            assertThat(resultMessage.type().version()).isEqualTo("1");
            assertThat(resultMessage.payloadAs(byte[].class)).isEqualTo(expectedPayload.getBytes());
            assertThat(resultMessage.metadata()).containsEntry("m", "v");
            assertThat(resultMessage.payloadAs(String.class)).isEqualTo(expectedPayload);

            verify(converter).convert(resultMessage.payload(), (Type) String.class);
        }

        @Test
        void convertsCommandResponseWithEmptyRevisionToDefaultVersion() {
            // given
            var response = CommandResponse.newBuilder()
                                          .setMessageIdentifier(messageIdentifier)
                                          .setPayload(SerializedObject.newBuilder()
                                                                      .setType("java.lang.String")
                                                                      .setData(ByteString.copyFrom("ok".getBytes()))
                                                                      .build())
                                          .build();

            // when
            var resultMessage = CommandConverter.convertCommandResponse(response, converter)
                                                .orTimeout(1, TimeUnit.SECONDS)
                                                .join();

            // then
            assertThat(resultMessage.type().version()).isEqualTo(MessageType.DEFAULT_VERSION);
        }

        @Test
        void convertsCommandResponseWithoutPayloadTypeToEmptyResult() {
            // given a response without payload, an empty payload type marks an empty result
            var response = CommandResponse.newBuilder()
                                          .setMessageIdentifier(messageIdentifier)
                                          .build();

            // when
            var future = CommandConverter.convertCommandResponse(response, converter);

            // then
            assertThat(future).succeedsWithin(Duration.ofSeconds(1)).isNull();
        }

        @Test
        void convertsErrorCommandResponseToFailedFuture() {
            // given
            var response = CommandResponse.newBuilder()
                                          .setMessageIdentifier(messageIdentifier)
                                          .setErrorCode("AXONIQ-4002")
                                          .setErrorMessage(ErrorMessage.newBuilder().setMessage("boom").build())
                                          .build();

            // when
            var future = CommandConverter.convertCommandResponse(response, converter);

            // then
            assertThat(future).failsWithin(Duration.ofSeconds(1))
                              .withThrowableOfType(ExecutionException.class)
                              .withCauseInstanceOf(CommandExecutionException.class)
                              .withMessageContaining("boom");
        }

        @Test
        void convertsErrorCommandResponseWithPayloadToFailedFutureCarryingConvertibleDetails() {
            // given
            String expectedDetails = "converted details";
            var response = CommandResponse.newBuilder()
                                          .setMessageIdentifier(messageIdentifier)
                                          .setErrorCode("AXONIQ-4002")
                                          .setErrorMessage(ErrorMessage.newBuilder().setMessage("boom").build())
                                          .setPayload(SerializedObject.newBuilder()
                                                                      .setType("java.lang.String")
                                                                      .setData(ByteString.copyFromUtf8(expectedDetails))
                                                                      .build())
                                          .build();

            // when
            var result = CommandConverter.convertCommandResponse(response, converter);

            // then
            assertThat(result).failsWithin(Duration.ofSeconds(1));
            Throwable resultThrowable = result.exceptionNow();
            assertThat(resultThrowable).isInstanceOf(CommandExecutionException.class);
            Optional<String> optionalDetails = ((CommandExecutionException) resultThrowable).getDetails(String.class);
            assertThat(optionalDetails).isPresent();
            assertThat(optionalDetails).hasValue(expectedDetails);

            verify(converter).convert(expectedDetails.getBytes(), (Type) String.class);
        }
    }

    @Nested
    class ConvertCommand {

        @Test
        void convertsCommandToCommandMessage() {
            // given
            var grpcCommand = Command.newBuilder()
                                     .setMessageIdentifier(messageIdentifier)
                                     .setName("CommandType")
                                     .setPayload(SerializedObject.newBuilder()
                                                                 .setType("CommandType")
                                                                 .setRevision("1")
                                                                 .setData(ByteString.copyFrom(payload))
                                                                 .build())
                                     .putMetaData("k", MetaDataValue.newBuilder().setTextValue("v").build())
                                     .addProcessingInstructions(createProcessingInstruction(ROUTING_KEY, "routingKey"))
                                     .addProcessingInstructions(createProcessingInstruction(PRIORITY, 5))
                                     .build();

            // when
            var commandMessage = CommandConverter.convertCommand(grpcCommand, converter);

            // then
            assertThat(commandMessage.identifier()).isEqualTo(messageIdentifier);
            assertThat(commandMessage.type().name()).isEqualTo("CommandType");
            assertThat(commandMessage.type().version()).isEqualTo("1");
            assertThat(commandMessage.payloadAs(byte[].class)).isEqualTo(payload);
            assertThat(commandMessage.metadata()).containsEntry("k", "v");
            assertThat(commandMessage.routingKey()).hasValue("routingKey");
            assertThat(commandMessage.priority()).hasValue(5);
            assertThat(commandMessage.payloadAs(String.class)).isEqualTo(stringPayload);

            verify(converter).convert(commandMessage.payload(), (Type) String.class);
        }

        @Test
        void convertsCommandWithoutRevisionAndProcessingInstructionsUsingDefaults() {
            // given
            var grpcCommand = Command.newBuilder()
                                     .setMessageIdentifier(messageIdentifier)
                                     .setPayload(SerializedObject.newBuilder()
                                                                 .setType("CommandType")
                                                                 .setData(ByteString.copyFrom(payload))
                                                                 .build())
                                     .build();

            // when
            var commandMessage = CommandConverter.convertCommand(grpcCommand, converter);

            // then absent revision defaults to the default version and absent priority to the lowest priority
            assertThat(commandMessage.type().version()).isEqualTo(MessageType.DEFAULT_VERSION);
            assertThat(commandMessage.routingKey()).isEmpty();
            assertThat(commandMessage.priority()).hasValue(0);
        }
    }

    @Nested
    class ConvertResultMessage {

        @Test
        void convertsResultMessageToCommandResponse() {
            // given
            var type = new MessageType("java.lang.String", "1");
            var resultMessage = new GenericCommandResultMessage(
                    new GenericMessage(messageIdentifier, type, "ok".getBytes(), Map.of("m", "v"))
            );

            // when
            var response = CommandConverter.convertResultMessage(resultMessage, "req-1");

            // then
            assertThat(response.getMessageIdentifier()).isEqualTo(messageIdentifier);
            assertThat(response.getRequestIdentifier()).isEqualTo("req-1");
            assertThat(response.getPayload().getType()).isEqualTo("java.lang.String");
            assertThat(response.getPayload().getRevision()).isEqualTo("1");
            assertThat(response.getPayload().getData().toByteArray()).isEqualTo("ok".getBytes());
            assertThat(response.getMetaDataMap().get("m").getTextValue()).isEqualTo("v");
        }

        @Test
        void convertsNullResultMessageToEmptyCommandResponse() {
            // when
            var response = CommandConverter.convertResultMessage(null, "req-1");

            // then
            assertThat(response.getRequestIdentifier()).isEqualTo("req-1");
            assertThat(response.getMessageIdentifier()).isNotBlank();
            assertThat(response.hasPayload()).isFalse();
        }

        @Test
        void convertsResultMessageWithNullPayloadToEmptyPayloadData() {
            // given
            var resultMessage =
                    new GenericCommandResultMessage(new MessageType("java.lang.String", "1"), (Object) null);

            // when
            var response = CommandConverter.convertResultMessage(resultMessage, "req-1");

            // then
            assertThat(response.getPayload().getType()).isEqualTo("java.lang.String");
            assertThat(response.getPayload().getData().isEmpty()).isTrue();
        }

        @Test
        void convertResultMessageThrowsOnNonByteArrayPayload() {
            // given
            var resultMessage =
                    new GenericCommandResultMessage(new MessageType("java.lang.String", "1"), "not-bytes");

            // when / then
            assertThatThrownBy(() -> CommandConverter.convertResultMessage(resultMessage, "req-1"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Payload must be of type byte[]");
        }
    }
}
