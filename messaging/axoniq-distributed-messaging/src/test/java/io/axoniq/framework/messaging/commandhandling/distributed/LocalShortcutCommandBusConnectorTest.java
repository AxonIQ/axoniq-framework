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
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link LocalShortcutCommandBusConnector}.
 *
 * @author Allard Buijze
 */
class LocalShortcutCommandBusConnectorTest {

    private final CommandBusConnector delegate = mock(CommandBusConnector.class);
    private final CommandBusConnector.Handler localHandler = mock(CommandBusConnector.Handler.class);
    private final ComponentDescriptor componentDescriptor = mock(ComponentDescriptor.class);

    private CommandMessage command;
    private QualifiedName commandName;

    @BeforeEach
    void setUp() {
        command = asCommandMessage("command");
        commandName = command.type().qualifiedName();
    }

    private LocalShortcutCommandBusConnector connectorFor(LocalCommandDispatchPredicate predicate) {
        return new LocalShortcutCommandBusConnector(delegate, predicate);
    }

    @Test
    void dispatchesToLocalHandlerWhenSubscribedAndPredicateMatches() {
        LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> true);
        connector.onIncomingCommand(localHandler);
        connector.subscribe(commandName, 100);

        CommandResultMessage resultMessage = asCommandResultMessage("result");
        answerWithSuccess(resultMessage);

        CompletableFuture<CommandResultMessage> result = connector.dispatch(command, null);

        assertThat(result).isCompletedWithValue(resultMessage);
        verify(localHandler).handle(eq(command), any());
        verify(delegate, never()).dispatch(any(), any());
    }

    @Test
    void propagatesLocalHandlerErrorWhenDispatchingLocally() {
        LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> true);
        connector.onIncomingCommand(localHandler);
        connector.subscribe(commandName, 100);

        RuntimeException failure = new RuntimeException("handling failed");
        doAnswer(invocation -> {
            invocation.getArgument(1, CommandBusConnector.ResultCallback.class).onError(failure);
            return null;
        }).when(localHandler).handle(eq(command), any());

        CompletableFuture<CommandResultMessage> result = connector.dispatch(command, null);

        assertThat(result).isCompletedExceptionally();
        assertThatThrownBy(result::get).hasCause(failure);
        verify(delegate, never()).dispatch(any(), any());
    }

    @Test
    void routesThroughDelegateWhenPredicateDoesNotMatch() {
        LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> false);
        connector.onIncomingCommand(localHandler);
        connector.subscribe(commandName, 100);

        CompletableFuture<CommandResultMessage> expected =
                CompletableFuture.completedFuture(asCommandResultMessage("result"));
        when(delegate.dispatch(command, null)).thenReturn(expected);

        CompletableFuture<CommandResultMessage> result = connector.dispatch(command, null);

        assertThat(result).isSameAs(expected);
        verify(delegate).dispatch(command, null);
        verify(localHandler, never()).handle(any(), any());
    }

    @Test
    void routesThroughDelegateWhenCommandNotLocallySubscribed() {
        LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> true);
        connector.onIncomingCommand(localHandler);
        // No matching subscription registered.

        CompletableFuture<CommandResultMessage> expected =
                CompletableFuture.completedFuture(asCommandResultMessage("result"));
        when(delegate.dispatch(command, null)).thenReturn(expected);

        CompletableFuture<CommandResultMessage> result = connector.dispatch(command, null);

        assertThat(result).isSameAs(expected);
        verify(delegate).dispatch(command, null);
        verify(localHandler, never()).handle(any(), any());
    }

    @Test
    void routesThroughDelegateWhenNoLocalHandlerRegistered() {
        LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> true);
        connector.subscribe(commandName, 100);
        // onIncomingCommand never called, so no local handler is available yet.

        CompletableFuture<CommandResultMessage> expected =
                CompletableFuture.completedFuture(asCommandResultMessage("result"));
        when(delegate.dispatch(command, null)).thenReturn(expected);

        CompletableFuture<CommandResultMessage> result = connector.dispatch(command, null);

        assertThat(result).isSameAs(expected);
        verify(delegate).dispatch(command, null);
    }

    @Test
    void unsubscribeStopsLocalDispatch() {
        LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> true);
        connector.onIncomingCommand(localHandler);
        connector.subscribe(commandName, 100);
        connector.unsubscribe(commandName);

        CompletableFuture<CommandResultMessage> expected =
                CompletableFuture.completedFuture(asCommandResultMessage("result"));
        when(delegate.dispatch(command, null)).thenReturn(expected);

        CompletableFuture<CommandResultMessage> result = connector.dispatch(command, null);

        assertThat(result).isSameAs(expected);
        verify(delegate).unsubscribe(commandName);
        verify(localHandler, never()).handle(any(), any());
    }

    @Test
    void passesProcessingContextToPredicate() {
        var seenContext = new java.util.concurrent.atomic.AtomicReference<Object>("unset");
        LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> {
            seenContext.set(ctx);
            return false;
        });
        connector.onIncomingCommand(localHandler);
        connector.subscribe(commandName, 100);
        when(delegate.dispatch(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        connector.dispatch(command, null);

        assertThat(seenContext.get()).isNull();
    }

    @Test
    void subscribeAndOnIncomingCommandDelegateToWrappedConnector() {
        LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> false);
        when(delegate.subscribe(commandName, 100)).thenReturn(CompletableFuture.completedFuture(null));
        when(delegate.unsubscribe(commandName)).thenReturn(true);

        connector.subscribe(commandName, 100);
        connector.onIncomingCommand(localHandler);
        boolean unsubscribed = connector.unsubscribe(commandName);

        verify(delegate).subscribe(commandName, 100);
        verify(delegate).onIncomingCommand(localHandler);
        verify(delegate).unsubscribe(commandName);
        assertThat(unsubscribed).isTrue();
    }

    @Test
    void describeToDescribesWrapperOfDelegate() {
        LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> false);

        connector.describeTo(componentDescriptor);

        verify(componentDescriptor).describeWrapperOf(delegate);
    }

    private void answerWithSuccess(CommandResultMessage resultMessage) {
        doAnswer(invocation -> {
            invocation.getArgument(1, CommandBusConnector.ResultCallback.class).onSuccess(resultMessage);
            return null;
        }).when(localHandler).handle(eq(command), any());
    }

    private CommandMessage asCommandMessage(String payload) {
        return new GenericCommandMessage(MessageType.fromString("commandmessage#1.0"), payload);
    }

    private CommandResultMessage asCommandResultMessage(String payload) {
        return new GenericCommandResultMessage(MessageType.fromString("commandresult#1.0"), payload);
    }
}
