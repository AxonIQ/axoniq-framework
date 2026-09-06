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
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.ProcessingKey;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import org.axonframework.common.lifecycle.ShutdownInProgressException;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link AxonServerCommandBusConnector}.
 *
 * @author Jens Mayer
 */
class AxonServerCommandBusConnectorTest {

    private static final String TEST_CLIENT_ID = "my-client-id";
    private static final String TEST_COMPONENT_NAME = "my-component-name";
    private static final QualifiedName ANY_TEST_COMMAND_NAME = new QualifiedName("TestCommand");
    private static final String ANY_TEST_MESSAGE_ID = "test-message-id";
    private static final String ANY_TEST_COMMAND_TYPE = "TestCommandType";
    private static final String ANY_TEST_REVISION = "1.0";
    private static final MessageType ANY_TEST_TYPE = new MessageType(ANY_TEST_COMMAND_TYPE, ANY_TEST_REVISION);
    private static final byte[] ANY_TEST_PAYLOAD = "test-payload".getBytes();
    private static final int ANY_TEST_LOAD_FACTOR = 100;
    private static final int ANY_TEST_PRIORITY = 5;
    private static final String ANY_TEST_ROUTING_KEY = "test-routing-key";

    private AxonServerConnection connection;
    private CommandChannel commandChannel;
    private MessageConverter converter;

    private AxonServerCommandBusConnector testSubject;

    @BeforeEach
    void setUp() {
        connection = mock(AxonServerConnection.class);
        commandChannel = mock(CommandChannel.class);
        converter = mock(MessageConverter.class);
        when(connection.commandChannel()).thenReturn(commandChannel);

        AxonServerConfiguration serverConfig = new AxonServerConfiguration();
        serverConfig.setClientId(TEST_CLIENT_ID);
        serverConfig.setComponentName(TEST_COMPONENT_NAME);
        testSubject = new AxonServerCommandBusConnector(connection, serverConfig, converter);
    }

    @Test
    void constructionWithConnectionNullRefFails() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> new AxonServerCommandBusConnector(null, new AxonServerConfiguration()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructionWithAxonServerConfigurationNullRefFails() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> new AxonServerCommandBusConnector(connection, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void dispatchingCommandMessageWithInvalidPayloadTypeFails() {
        CommandMessage command = new GenericCommandMessage(
                new GenericMessage(ANY_TEST_MESSAGE_ID, ANY_TEST_TYPE, "invalid-payload", new HashMap<>())
        );
        assertThatThrownBy(() -> testSubject.dispatch(command, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void dispatchingCommandMessageWithValidPayloadResultsToResponse() {
        // Arrange
        String expectedPayload = "expected-payload";
        CommandMessage command = createTestCommandMessage();
        CommandResponse response = createSuccessfulCommandResponse();
        ArgumentCaptor<Command> commandCaptor = ArgumentCaptor.forClass(Command.class);
        when(commandChannel.sendCommand(commandCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(response));
        when(converter.convert(any(), eq((Type) String.class)))
                .thenReturn(expectedPayload);

        // Act
        CompletableFuture<CommandResultMessage> result = testSubject.dispatch(command, null);

        // Assert
        Command dispatchedCommand = commandCaptor.getValue();
        assertThat(dispatchedCommand.getClientId()).isEqualTo(TEST_CLIENT_ID);
        assertThat(dispatchedCommand.getComponentName()).isEqualTo(TEST_COMPONENT_NAME);
        assertThat(result)
                .isNotNull()
                .isCompleted();
        assertThat(result.join().payloadType()).isEqualTo(byte[].class);
        assertThat(result.join().payloadAs(String.class)).isEqualTo(expectedPayload);

        verify(commandChannel).sendCommand(any(Command.class));
        verify(converter).convert(any(), eq((Type) String.class));
    }

    @Test
    void dispatchingBuildsCorrectOutgoingCommand() {
        // Arrange
        Map<String, String> metadata = Map.of("key1", "value1", "key2", "value2");
        CommandMessage command = new GenericCommandMessage(
                new GenericMessage(ANY_TEST_MESSAGE_ID, ANY_TEST_TYPE, ANY_TEST_PAYLOAD, metadata),
                ANY_TEST_ROUTING_KEY,
                ANY_TEST_PRIORITY
        );

        when(commandChannel.sendCommand(any(Command.class)))
                .thenReturn(CompletableFuture.completedFuture(createSuccessfulCommandResponse()));

        ArgumentCaptor<Command> commandCaptor = ArgumentCaptor.forClass(Command.class);

        // Act
        testSubject.dispatch(command, null);

        // Assert
        verify(commandChannel).sendCommand(commandCaptor.capture());
        Command sentCommand = commandCaptor.getValue();

        assertThat(sentCommand.getMessageIdentifier()).isEqualTo(ANY_TEST_MESSAGE_ID);
        assertThat(sentCommand.getName()).isEqualTo(ANY_TEST_COMMAND_TYPE);
        assertThat(sentCommand.getPayload().getType()).isEqualTo(ANY_TEST_COMMAND_TYPE);
        assertThat(sentCommand.getPayload().getRevision()).isEqualTo(ANY_TEST_REVISION);
        assertThat(sentCommand.getPayload().getData().toByteArray()).containsExactly(ANY_TEST_PAYLOAD);
        assertThat(sentCommand.getMetaDataCount()).isEqualTo(2);
        assertThat(sentCommand.getProcessingInstructionsList()).hasSizeGreaterThanOrEqualTo(2); // Priority and routing key
    }

    @Test
    void dispatchingWithEmptyPriorityDoesNotAddPriorityInstruction() {
        // Arrange
        CommandMessage command = createTestCommandMessage();

        when(commandChannel.sendCommand(any(Command.class)))
                .thenReturn(CompletableFuture.completedFuture(createSuccessfulCommandResponse()));

        ArgumentCaptor<Command> commandCaptor = ArgumentCaptor.forClass(Command.class);

        // Act
        testSubject.dispatch(command, null);

        // Assert
        verify(commandChannel).sendCommand(commandCaptor.capture());
        Command sentCommand = commandCaptor.getValue();

        assertThat(sentCommand.getProcessingInstructionsList().stream()
                               .noneMatch(pi -> pi.getKey() == ProcessingKey.PRIORITY)).isTrue();
    }

    @Test
    void dispatchingHandlesErrorResponse() {
        // Arrange
        CommandMessage command = createTestCommandMessage();
        CommandResponse errorResponse = CommandResponse.newBuilder()
                                                       .setMessageIdentifier(UUID.randomUUID().toString())
                                                       .setErrorCode("COMMAND_EXECUTION_ERROR")
                                                       .setErrorMessage(ErrorMessage.newBuilder().setMessage(
                                                               "Command execution error").build()
                                                       )
                                                       .build();

        when(commandChannel.sendCommand(any(Command.class)))
                .thenReturn(CompletableFuture.completedFuture(errorResponse));

        // Act
        CompletableFuture<CommandResultMessage> result = testSubject.dispatch(command, null);

        // Assert
        assertThat(result.isCompletedExceptionally()).isTrue();
    }

    @Test
    void dispatchingHandlesEmptyPayloadResponse() {
        // Arrange
        CommandMessage command = createTestCommandMessage();
        CommandResponse response = CommandResponse.newBuilder()
                                                  .setMessageIdentifier(UUID.randomUUID().toString())
                                                  .setPayload(SerializedObject.newBuilder()
                                                                              .setType("")
                                                                              .setRevision("")
                                                                              .setData(ByteString.EMPTY)
                                                                              .build())
                                                  .build();

        when(commandChannel.sendCommand(any(Command.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        // Act
        CompletableFuture<CommandResultMessage> result = testSubject.dispatch(command, null);

        // Assert
        assertThat(result).isCompleted();
        CommandResultMessage resultMessage = result.join();
        assertThat(resultMessage).isNotNull();
        assertThat(resultMessage.payload()).isNull();
        assertThat(resultMessage.type()).isEqualTo(ANY_TEST_TYPE);
    }

    @Test
    void subscribeRegistersCommandHandlerWithCorrectParameters() {
        // Arrange
        Registration mockRegistration = mock(Registration.class);
        when(commandChannel.registerCommandHandler(any(), eq(ANY_TEST_LOAD_FACTOR), eq(ANY_TEST_COMMAND_NAME.name())))
                .thenReturn(mockRegistration);

        // Act
        testSubject.subscribe(ANY_TEST_COMMAND_NAME, ANY_TEST_LOAD_FACTOR);

        // Assert
        assertThat(getSubscriptions(testSubject)).containsEntry(ANY_TEST_COMMAND_NAME, mockRegistration);
    }

    @Test
    void subscribeWithNegativeLoadFactorThrowsException() {
        assertThatThrownBy(() -> testSubject.subscribe(ANY_TEST_COMMAND_NAME, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void subscribeWithAsyncRegistrationWaitsForAcknowledgment() {
        // Arrange
        Registration asyncRegistration = mock(Registration.class);
        when(commandChannel.registerCommandHandler(any(), eq(ANY_TEST_LOAD_FACTOR), eq(ANY_TEST_COMMAND_NAME.name())))
                .thenReturn(asyncRegistration);

        // Act
        testSubject.subscribe(ANY_TEST_COMMAND_NAME, ANY_TEST_LOAD_FACTOR);

        // Assert
        verify(asyncRegistration).onAck(any(Runnable.class));
    }

    @Test
    void unsubscribeRemovesAndCancelsRegistration() {
        // Arrange
        Registration mockRegistration = mock(Registration.class);
        when(commandChannel.registerCommandHandler(any(), eq(ANY_TEST_LOAD_FACTOR), eq(ANY_TEST_COMMAND_NAME.name())))
                .thenReturn(mockRegistration);

        testSubject.subscribe(ANY_TEST_COMMAND_NAME, ANY_TEST_LOAD_FACTOR);

        // Act
        boolean result = testSubject.unsubscribe(ANY_TEST_COMMAND_NAME);

        // Assert
        assertThat(result).isTrue();
        verify(mockRegistration).cancel();
        assertThat(getSubscriptions(testSubject)).doesNotContainKey(ANY_TEST_COMMAND_NAME);

        // Second unsubscribe should return false
        assertThat(testSubject.unsubscribe(ANY_TEST_COMMAND_NAME)).isFalse();
    }

    @Test
    void unsubscribeNonExistentCommandReturnsFalse() {
        // Act
        boolean result = testSubject.unsubscribe(ANY_TEST_COMMAND_NAME);

        // Assert
        assertThat(result).isFalse();
    }

    @Test
    void onIncomingCommandSetsHandler() {
        // Arrange
        CommandBusConnector.Handler handler = mock(CommandBusConnector.Handler.class);

        // Act
        testSubject.onIncomingCommand(handler);

        // Assert
        assertThat(getIncomingHandler(testSubject)).isSameAs(handler);
    }

    @Test
    void disconnectInvokesPrepareDisconnectOnCommandChannel() {
        when(commandChannel.prepareDisconnect()).thenReturn(CompletableFuture.completedFuture(null));
        when(connection.isConnected()).thenReturn(true);

        testSubject.disconnect();

        verify(commandChannel).prepareDisconnect();
    }

    @Test
    void disconnectDoesNotCloseTheConnectionOnceCommandChannelPreparationCompletes() {
        when(commandChannel.prepareDisconnect()).thenReturn(CompletableFuture.completedFuture(null));
        when(connection.isConnected()).thenReturn(true);

        testSubject.disconnect().join();

        verify(connection, never()).disconnect();
    }

    @Test
    void afterShutdownDispatchingAnShutdownInProgressExceptionIsThrownOnDispatchInvocation() {
        CommandMessage testCommand = new GenericCommandMessage(ANY_TEST_TYPE, ANY_TEST_PAYLOAD);

        // when...
        testSubject.shutdownDispatching();
        // then...
        assertThatThrownBy(() -> testSubject.dispatch(testCommand, null))
                .isInstanceOf(ShutdownInProgressException.class);
    }

    @Test
    void shutdownDispatchingWaitsForCommandsInTransitToComplete() {
        CommandMessage testCommand = new GenericCommandMessage(ANY_TEST_TYPE, ANY_TEST_PAYLOAD);
        CompletableFuture<CommandResponse> testResponseFuture = new CompletableFuture<>();
        AtomicBoolean handled = new AtomicBoolean(false);
        when(commandChannel.sendCommand(any())).thenReturn(testResponseFuture);

        // given ...
        testSubject.dispatch(testCommand, null)
                   .whenComplete((result, exception) -> handled.set(true));
        Thread.ofVirtual()
              .name("Return Command Response")
              .start(() -> {
                  try {
                      Thread.sleep(200);
                      testResponseFuture.complete(mock(CommandResponse.class));
                  } catch (InterruptedException e) {
                      fail(e.getMessage());
                      throw new RuntimeException(e);
                  }
              });

        // when ...
        CompletableFuture<Void> dispatchingHasShutdown = testSubject.shutdownDispatching();

        // then ... Wait on the shutdownDispatching-thread, after which the command should have been handled
        dispatchingHasShutdown.join();
        await("Dispatch completion").atMost(Duration.ofSeconds(1))
                                    .pollDelay(Duration.ofMillis(25))
                                    .untilAsserted(() -> {
                                        assertThat(handled.get()).isTrue();
                                        assertThat(dispatchingHasShutdown.isDone()).isTrue();
                                    });
    }

    @Nested
    class GracefulShutdown {

        @Test
        void disconnectCompletesWhenIncomingCommandsAreHandled() {
            when(connection.isConnected()).thenReturn(true);
            CompletableFuture<Void> disconnectCompletion = new CompletableFuture<>();
            when(commandChannel.prepareDisconnect()).thenReturn(disconnectCompletion);

            AtomicReference<CommandBusConnector.ResultCallback> resultCallback = new AtomicReference<>();
            testSubject.onIncomingCommand((commandMessage, callback) -> resultCallback.set(callback));

            getIncomingHandler(testSubject).handle(createTestCommandMessage(), mock());

            assertThat(resultCallback.get()).as("Command was not received").isNotNull();
            CompletableFuture<Void> result = testSubject.disconnect();
            assertThat(result).isNotNull();
            verify(commandChannel).prepareDisconnect();
            assertThat(result.isDone()).isFalse();

            disconnectCompletion.complete(null);

            resultCallback.get().onSuccess(new GenericCommandResultMessage(ANY_TEST_TYPE, ANY_TEST_PAYLOAD));
            assertThat(result.isDone()).isTrue();
        }

        @Test
        void disconnectCompletesOnPrepareWhenNoActiveCommandsAvailable() {
            when(connection.isConnected()).thenReturn(true);
            CompletableFuture<Void> disconnectCompletion = new CompletableFuture<>();
            when(commandChannel.prepareDisconnect()).thenReturn(disconnectCompletion);

            CompletableFuture<Void> result = testSubject.disconnect();
            assertThat(result).isNotNull();
            verify(commandChannel).prepareDisconnect();
            assertThat(result.isDone()).isFalse();

            disconnectCompletion.complete(null);

            assertThat(result.isDone()).isTrue();
        }
    }

    @Nested
    class FutureResultCallback {

        @Test
        void onErrorCompletesResponseSuccessfullyWithErrorCodeMessageAndDetailsPayload() {
            when(converter.convert("some details", byte[].class)).thenReturn("some details".getBytes());

            CompletableFuture<CommandResponse> result =
                    triggerOnError(new CommandExecutionException("boom", null, "some details"));

            assertThat(result).isCompleted();
            CommandResponse response = result.join();
            assertThat(response.getErrorMessage().getMessage()).isEqualTo("boom");
            assertThat(response.getErrorCode()).isNotEmpty();
            assertThat(response.hasPayload()).isTrue();
            assertThat(response.getPayload().getData().toStringUtf8()).isEqualTo("some details");
            assertThat(response.getPayload().getType()).isEqualTo(String.class.getName());
        }

        @Test
        void onErrorWithoutHandlerExecutionDetailsCompletesResponseWithoutPayload() {
            CompletableFuture<CommandResponse> result = triggerOnError(new RuntimeException("boom"));

            assertThat(result).isCompleted();
            CommandResponse response = result.join();
            assertThat(response.getErrorMessage().getMessage()).isEqualTo("boom");
            assertThat(response.hasPayload()).isFalse();
        }

        private CompletableFuture<CommandResponse> triggerOnError(Throwable cause) {
            Registration mockRegistration = mock(Registration.class);
            //noinspection unchecked
            ArgumentCaptor<Function<Command, CompletableFuture<CommandResponse>>> handlerCaptor =
                    ArgumentCaptor.forClass(Function.class);
            when(commandChannel.registerCommandHandler(handlerCaptor.capture(),
                                                       eq(ANY_TEST_LOAD_FACTOR),
                                                       eq(ANY_TEST_COMMAND_NAME.name())))
                    .thenReturn(mockRegistration);
            testSubject.subscribe(ANY_TEST_COMMAND_NAME, ANY_TEST_LOAD_FACTOR);

            AtomicReference<CommandBusConnector.ResultCallback> resultCallback = new AtomicReference<>();
            testSubject.onIncomingCommand((commandMessage, callback) -> resultCallback.set(callback));

            Command command = Command.newBuilder()
                                     .setName(ANY_TEST_COMMAND_NAME.name())
                                     .setMessageIdentifier(ANY_TEST_MESSAGE_ID)
                                     .setPayload(SerializedObject.newBuilder()
                                                                 .setType(ANY_TEST_COMMAND_TYPE)
                                                                 .setRevision(ANY_TEST_REVISION)
                                                                 .setData(ByteString.copyFrom(ANY_TEST_PAYLOAD)))
                                     .build();

            CompletableFuture<CommandResponse> result = handlerCaptor.getValue().apply(command);
            resultCallback.get().onError(cause);
            return result;
        }
    }

    // Helpers
    private CommandMessage createTestCommandMessage() {
        return new GenericCommandMessage(new GenericMessage(ANY_TEST_MESSAGE_ID,
                                                            ANY_TEST_TYPE,
                                                            ANY_TEST_PAYLOAD,
                                                            new HashMap<>()));
    }

    private CommandResponse createSuccessfulCommandResponse() {
        return CommandResponse.newBuilder()
                              .setMessageIdentifier(UUID.randomUUID().toString())
                              .setPayload(SerializedObject.newBuilder()
                                                          .setType("ResponseType")
                                                          .setRevision("1.0")
                                                          .setData(ByteString.copyFrom("response-payload".getBytes()))
                                                          .build())
                              .putMetaData("responseKey",
                                           MetaDataValue.newBuilder().setTextValue("responseValue").build())
                              .build();
    }

    private Map<QualifiedName, Registration> getSubscriptions(AxonServerCommandBusConnector instance) {
        try {
            Field field = instance.getClass().getDeclaredField("subscriptions");
            field.setAccessible(true);

            //noinspection unchecked
            return (Map<QualifiedName, Registration>) field.get(instance);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private CommandBusConnector.Handler getIncomingHandler(AxonServerCommandBusConnector instance) {
        try {
            Field field = AxonServerCommandBusConnector.class.getDeclaredField("incomingHandler");
            field.setAccessible(true);
            return (CommandBusConnector.Handler) field.get(instance);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
