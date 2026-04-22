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

package io.axoniq.framework.messaging.commandhandling.distributed;

import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.conversion.DelegatingMessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.conversion.Converter;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.lang.reflect.Type;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class PayloadConvertingCommandBusConnectorTest {

    private static final String ORIGINAL_PAYLOAD = "original";
    private static final byte[] CONVERTED_PAYLOAD = ORIGINAL_PAYLOAD.getBytes();
    private static final MessageType COMMAND_TYPE = new MessageType("TestCommand");

    private CommandBusConnector mockDelegate;
    private Converter mockConverter;
    private PayloadConvertingCommandBusConnector testSubject;

    @BeforeEach
    void setUp() {
        mockDelegate = mock(CommandBusConnector.class);
        mockConverter = mock(Converter.class);
        testSubject = new PayloadConvertingCommandBusConnector(
                mockDelegate, new DelegatingMessageConverter(mockConverter), byte[].class
        );
    }

    @Test
    void constructorRequiresNonNullDelegate() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> new PayloadConvertingCommandBusConnector(
                null, new DelegatingMessageConverter(mockConverter), byte[].class
        )).isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructorRequiresNonNullConverter() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> new PayloadConvertingCommandBusConnector(
                mockDelegate, null, byte[].class
        )).isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructorRequiresNonNullTargetType() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> new PayloadConvertingCommandBusConnector(
                mockDelegate, new DelegatingMessageConverter(mockConverter), null
        )).isInstanceOf(NullPointerException.class);
    }

    @Test
    void dispatchConvertsCommandPayloadBeforeDelegating() {
        // Given
        CommandMessage originalCommand = new GenericCommandMessage(COMMAND_TYPE, ORIGINAL_PAYLOAD);
        ProcessingContext context = mock(ProcessingContext.class);

        when(mockConverter.convert(ORIGINAL_PAYLOAD, (Type) byte[].class)).thenReturn(CONVERTED_PAYLOAD);
        when(mockDelegate.dispatch(any(CommandMessage.class), eq(context)))
                .thenReturn(CompletableFuture.completedFuture(mock(CommandResultMessage.class)));

        // When
        CompletableFuture<CommandResultMessage> result = testSubject.dispatch(originalCommand, context);

        // Then
        assertThat(result).isNotNull();
        ArgumentCaptor<CommandMessage> commandCaptor = ArgumentCaptor.forClass(CommandMessage.class);
        verify(mockDelegate).dispatch(commandCaptor.capture(), eq(context));

        CommandMessage capturedCommand = commandCaptor.getValue();
        assertThat((byte[]) capturedCommand.payload()).containsExactly(CONVERTED_PAYLOAD);
        assertThat(capturedCommand.type()).isEqualTo(originalCommand.type());
        assertThat(capturedCommand.metadata()).isEqualTo(originalCommand.metadata());
    }

    @Test
    void convertingCallbackConvertsSuccessResultMessage() {
        // Given
        CommandBusConnector.Handler originalHandler = mock(CommandBusConnector.Handler.class);
        testSubject.onIncomingCommand(originalHandler);

        ArgumentCaptor<CommandBusConnector.Handler> handlerCaptor =
                ArgumentCaptor.forClass(CommandBusConnector.Handler.class);
        verify(mockDelegate).onIncomingCommand(handlerCaptor.capture());

        CommandMessage testCommand = new GenericCommandMessage(COMMAND_TYPE, ORIGINAL_PAYLOAD);
        CommandBusConnector.ResultCallback originalCallback = mock(CommandBusConnector.ResultCallback.class);

        handlerCaptor.getValue().handle(testCommand, originalCallback);

        ArgumentCaptor<CommandBusConnector.ResultCallback> callbackCaptor =
                ArgumentCaptor.forClass(CommandBusConnector.ResultCallback.class);
        verify(originalHandler).handle(eq(testCommand), callbackCaptor.capture());

        CommandBusConnector.ResultCallback convertingCallback = callbackCaptor.getValue();

        // When
        CommandResultMessage resultMessage = new GenericCommandResultMessage(COMMAND_TYPE, ORIGINAL_PAYLOAD);
        when(mockConverter.convert(ORIGINAL_PAYLOAD, (Type) byte[].class)).thenReturn(CONVERTED_PAYLOAD);

        convertingCallback.onSuccess(resultMessage);

        // Then
        ArgumentCaptor<CommandResultMessage> messageCaptor = ArgumentCaptor.forClass(CommandResultMessage.class);
        verify(originalCallback).onSuccess(messageCaptor.capture());

        Message convertedMessage = messageCaptor.getValue();
        assertThat((byte[]) convertedMessage.payload()).containsExactly(CONVERTED_PAYLOAD);
        assertThat(convertedMessage.type()).isEqualTo(resultMessage.type());
        assertThat(convertedMessage.metadata()).isEqualTo(resultMessage.metadata());
    }

    @Test
    void convertingCallbackHandlesNullResultMessage() {
        // Given
        CommandBusConnector.Handler originalHandler = mock(CommandBusConnector.Handler.class);
        testSubject.onIncomingCommand(originalHandler);

        ArgumentCaptor<CommandBusConnector.Handler> handlerCaptor =
                ArgumentCaptor.forClass(CommandBusConnector.Handler.class);
        verify(mockDelegate).onIncomingCommand(handlerCaptor.capture());

        CommandMessage testCommand = new GenericCommandMessage(COMMAND_TYPE, ORIGINAL_PAYLOAD);
        CommandBusConnector.ResultCallback originalCallback = mock(CommandBusConnector.ResultCallback.class);

        handlerCaptor.getValue().handle(testCommand, originalCallback);

        ArgumentCaptor<CommandBusConnector.ResultCallback> callbackCaptor = ArgumentCaptor.forClass(CommandBusConnector.ResultCallback.class);
        verify(originalHandler).handle(eq(testCommand), callbackCaptor.capture());

        CommandBusConnector.ResultCallback convertingCallback = callbackCaptor.getValue();

        // When
        convertingCallback.onSuccess(null);

        // Then
        verify(originalCallback).onSuccess(null);
        verify(mockConverter, never()).convert(any(), any());
    }

    @Test
    void convertingCallbackHandlesMessageWithNullPayload() {
        // Given
        CommandBusConnector.Handler originalHandler = mock(CommandBusConnector.Handler.class);
        testSubject.onIncomingCommand(originalHandler);

        ArgumentCaptor<CommandBusConnector.Handler> handlerCaptor = ArgumentCaptor.forClass(CommandBusConnector.Handler.class);
        verify(mockDelegate).onIncomingCommand(handlerCaptor.capture());

        CommandMessage testCommand = new GenericCommandMessage(COMMAND_TYPE, ORIGINAL_PAYLOAD);
        CommandBusConnector.ResultCallback originalCallback = mock(CommandBusConnector.ResultCallback.class);

        handlerCaptor.getValue().handle(testCommand, originalCallback);

        ArgumentCaptor<CommandBusConnector.ResultCallback> callbackCaptor =
                ArgumentCaptor.forClass(CommandBusConnector.ResultCallback.class);
        verify(originalHandler).handle(eq(testCommand), callbackCaptor.capture());

        CommandBusConnector.ResultCallback convertingCallback = callbackCaptor.getValue();

        // When
        CommandResultMessage resultMessageWithNullPayload = new GenericCommandResultMessage(COMMAND_TYPE, null);
        convertingCallback.onSuccess(resultMessageWithNullPayload);

        // Then
        verify(originalCallback).onSuccess(resultMessageWithNullPayload);
        verify(mockConverter, never()).convert(any(), any());
    }

    @Test
    void preservesCommandMetadataWhenConverting() {
        // Given
        Metadata originalMetadata = Metadata.with("key", "value");
        CommandMessage originalCommand = new GenericCommandMessage(
                COMMAND_TYPE, ORIGINAL_PAYLOAD, originalMetadata);

        when(mockConverter.convert(ORIGINAL_PAYLOAD, byte[].class)).thenReturn(CONVERTED_PAYLOAD);
        when(mockDelegate.dispatch(any(CommandMessage.class), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(CommandResultMessage.class)));

        // When
        testSubject.dispatch(originalCommand, null);

        // Then
        ArgumentCaptor<CommandMessage> commandCaptor = ArgumentCaptor.forClass(CommandMessage.class);
        verify(mockDelegate).dispatch(commandCaptor.capture(), any());

        CommandMessage capturedCommand = commandCaptor.getValue();
        assertThat(capturedCommand.metadata()).isEqualTo(originalMetadata);
    }
}