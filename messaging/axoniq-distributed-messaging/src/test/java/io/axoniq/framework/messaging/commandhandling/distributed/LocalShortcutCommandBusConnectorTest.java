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

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link LocalShortcutCommandBusConnector}.
 *
 * @author Allard Buijze
 */
class LocalShortcutCommandBusConnectorTest {

    private final RecordingCommandBusConnector delegate = new RecordingCommandBusConnector();
    private final RecordingHandler localHandler = new RecordingHandler();

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

    @Nested
    class LocalDispatch {

        @Test
        void dispatchesToLocalHandlerWhenSubscribedAndPredicateMatches() {
            // given
            LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> true);
            connector.onIncomingCommand(localHandler);
            connector.subscribe(commandName, 100);

            CommandResultMessage resultMessage = asCommandResultMessage("result");
            localHandler.resultToReturn = resultMessage;

            // when
            CompletableFuture<CommandResultMessage> result = connector.dispatch(command, null);

            // then
            assertThat(result).isCompletedWithValue(resultMessage);
            assertThat(localHandler.handleCount).hasValue(1);
            assertThat(delegate.dispatchCount).hasValue(0);
        }

        @Test
        void propagatesLocalHandlerErrorWhenDispatchingLocally() {
            // given
            LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> true);
            connector.onIncomingCommand(localHandler);
            connector.subscribe(commandName, 100);

            RuntimeException failure = new RuntimeException("handling failed");
            localHandler.errorToRaise = failure;

            // when
            CompletableFuture<CommandResultMessage> result = connector.dispatch(command, null);

            // then
            assertThat(result).isCompletedExceptionally();
            assertThatThrownBy(result::get).hasCause(failure);
            assertThat(delegate.dispatchCount).hasValue(0);
        }
    }

    @Nested
    class DelegatedDispatch {

        @Test
        void routesThroughDelegateWhenPredicateDoesNotMatch() {
            // given
            LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> false);
            connector.onIncomingCommand(localHandler);
            connector.subscribe(commandName, 100);

            // when
            connector.dispatch(command, null);

            // then
            assertThat(delegate.dispatchCount).hasValue(1);
            assertThat(localHandler.handleCount).hasValue(0);
        }

        @Test
        void routesThroughDelegateWhenCommandNotLocallySubscribed() {
            // given
            LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> true);
            connector.onIncomingCommand(localHandler);
            // No matching subscription registered.

            // when
            connector.dispatch(command, null);

            // then
            assertThat(delegate.dispatchCount).hasValue(1);
            assertThat(localHandler.handleCount).hasValue(0);
        }

        @Test
        void routesThroughDelegateWhenNoLocalHandlerRegistered() {
            // given
            LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> true);
            connector.subscribe(commandName, 100);
            // onIncomingCommand never called, so no local handler is available yet.

            // when
            connector.dispatch(command, null);

            // then
            assertThat(delegate.dispatchCount).hasValue(1);
        }

        @Test
        void unsubscribeStopsLocalDispatch() {
            // given
            LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> true);
            connector.onIncomingCommand(localHandler);
            connector.subscribe(commandName, 100);
            connector.unsubscribe(commandName);

            // when
            connector.dispatch(command, null);

            // then
            assertThat(delegate.dispatchCount).hasValue(1);
            assertThat(localHandler.handleCount).hasValue(0);
            assertThat(delegate.unsubscribed).containsExactly(commandName);
        }

        @Test
        void passesProcessingContextToPredicate() {
            // given
            AtomicReference<Object> seenContext = new AtomicReference<>("unset");
            LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> {
                seenContext.set(ctx);
                return false;
            });
            connector.onIncomingCommand(localHandler);
            connector.subscribe(commandName, 100);

            // when
            connector.dispatch(command, null);

            // then
            assertThat(seenContext.get()).isNull();
        }
    }

    @Nested
    class Delegation {

        @Test
        void subscribeOnIncomingCommandAndUnsubscribeDelegateToWrappedConnector() {
            // given
            LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> false);

            // when
            connector.subscribe(commandName, 100);
            connector.onIncomingCommand(localHandler);
            boolean unsubscribed = connector.unsubscribe(commandName);

            // then
            assertThat(delegate.subscribed).containsExactly(commandName);
            assertThat(delegate.incomingHandler).isSameAs(localHandler);
            assertThat(delegate.unsubscribed).containsExactly(commandName);
            assertThat(unsubscribed).isTrue();
        }

        @Test
        void describeToDescribesWrapperOfDelegate() {
            // given
            LocalShortcutCommandBusConnector connector = connectorFor((c, ctx) -> false);
            RecordingComponentDescriptor descriptor = new RecordingComponentDescriptor();

            // when
            connector.describeTo(descriptor);

            // then
            assertThat(descriptor.properties).containsEntry("delegate", delegate);
        }
    }

    private CommandMessage asCommandMessage(String payload) {
        return new GenericCommandMessage(MessageType.fromString("command-message#1.0"), payload);
    }

    private CommandResultMessage asCommandResultMessage(String payload) {
        return new GenericCommandResultMessage(MessageType.fromString("command-result#1.0"), payload);
    }

    /**
     * Recording {@link CommandBusConnector} that counts dispatches and captures subscriptions, standing in for the
     * wrapped connector.
     */
    private static class RecordingCommandBusConnector implements CommandBusConnector {

        private final CompletableFuture<CommandResultMessage> dispatchResult = new CompletableFuture<>();
        private final AtomicInteger dispatchCount = new AtomicInteger();
        private final List<QualifiedName> subscribed = new ArrayList<>();
        private final List<QualifiedName> unsubscribed = new ArrayList<>();
        private Handler incomingHandler;

        @NonNull
        @Override
        public CompletableFuture<CommandResultMessage> dispatch(@NonNull CommandMessage command,
                                                                @Nullable ProcessingContext processingContext) {
            dispatchCount.incrementAndGet();
            return dispatchResult;
        }

        @NonNull
        @Override
        public CompletableFuture<Void> subscribe(@NonNull QualifiedName commandName, int loadFactor) {
            subscribed.add(commandName);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean unsubscribe(@NonNull QualifiedName commandName) {
            unsubscribed.add(commandName);
            return true;
        }

        @Override
        public void onIncomingCommand(@NonNull Handler handler) {
            this.incomingHandler = handler;
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("name", "RecordingCommandBusConnector");
        }
    }

    /**
     * Recording local {@link CommandBusConnector.Handler} that reports either a preset result or a preset error through
     * the {@link CommandBusConnector.ResultCallback}.
     */
    private static class RecordingHandler implements CommandBusConnector.Handler {

        private final AtomicInteger handleCount = new AtomicInteger();
        private CommandResultMessage resultToReturn;
        private @Nullable Throwable errorToRaise;

        @Override
        public void handle(@NonNull CommandMessage commandMessage,
                           CommandBusConnector.@NonNull ResultCallback callback) {
            handleCount.incrementAndGet();
            if (errorToRaise != null) {
                callback.onError(errorToRaise);
            } else {
                callback.onSuccess(resultToReturn);
            }
        }
    }

    /**
     * Recording {@link ComponentDescriptor} capturing the properties described to it, so the wrapper relationship can
     * be asserted without mocking.
     */
    private static class RecordingComponentDescriptor implements ComponentDescriptor {

        private final Map<String, Object> properties = new HashMap<>();

        @Override
        public void describeProperty(@NonNull String name, @Nullable Object object) {
            properties.put(name, object);
        }

        @Override
        public void describeProperty(@NonNull String name, @Nullable Collection<?> collection) {
            properties.put(name, collection);
        }

        @Override
        public void describeProperty(@NonNull String name, @Nullable Map<?, ?> map) {
            properties.put(name, map);
        }

        @Override
        public void describeProperty(@NonNull String name, @Nullable String value) {
            properties.put(name, value);
        }

        @Override
        public void describeProperty(@NonNull String name, @Nullable Long value) {
            properties.put(name, value);
        }

        @Override
        public void describeProperty(@NonNull String name, @Nullable Boolean value) {
            properties.put(name, value);
        }
    }
}
