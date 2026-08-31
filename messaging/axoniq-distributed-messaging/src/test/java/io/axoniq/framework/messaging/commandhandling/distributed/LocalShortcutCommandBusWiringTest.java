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

import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Wiring test that assembles a full {@link MessagingConfigurer} stack - {@link DistributedCommandBus} over a
 * {@link LocalShortcutCommandBusConnector} decorating a recording connector - and verifies end-to-end that commands
 * take the local shortcut exactly when the {@link LocalCommandDispatchPredicate} and a local subscription allow it, and
 * are routed through the connector otherwise.
 *
 * @author Allard Buijze
 */
class LocalShortcutCommandBusWiringTest {

    private static final QualifiedName SUBSCRIBED = new QualifiedName("io.axoniq.test.SubscribedCommand");
    private static final QualifiedName UNSUBSCRIBED = new QualifiedName("io.axoniq.test.UnsubscribedCommand");
    private static final String LOCAL_HANDLER_RESULT = "local-result";

    private RecordingCommandBusConnector connector;
    private AtomicBoolean localHandlerInvoked;

    @BeforeEach
    void setUp() {
        connector = new RecordingCommandBusConnector();
        localHandlerInvoked = new AtomicBoolean(false);
    }

    private CommandBus commandBusWith(LocalCommandDispatchPredicate predicate) {
        Configuration config =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> {
                                       cr.registerComponent(CommandBusConnector.class, c -> connector);
                                       cr.registerComponent(LocalCommandDispatchPredicate.class, c -> predicate);
                                   })
                                   .build();
        CommandBus commandBus = config.getComponent(CommandBus.class);
        commandBus.subscribe(SUBSCRIBED, (command, context) -> {
            localHandlerInvoked.set(true);
            return MessageStream.just(commandResultMessage(LOCAL_HANDLER_RESULT));
        });
        return commandBus;
    }

    @Test
    void shortcutsToLocalHandlerWhenSubscribedAndPredicateMatches() {
        // given
        CommandBus commandBus = commandBusWith((command, context) -> true);

        // when
        CompletableFuture<CommandResultMessage> result = commandBus.dispatch(commandMessage(SUBSCRIBED), null);

        // then
        assertThat(result).succeedsWithin(Duration.ofSeconds(5))
                          .satisfies(response -> assertThat(response.payload()).isEqualTo(LOCAL_HANDLER_RESULT));
        assertThat(localHandlerInvoked).isTrue();
        assertThat(connector.dispatchCount).hasValue(0);
    }

    @Test
    void routesThroughConnectorWhenCommandNotLocallySubscribed() {
        // given
        CommandBus commandBus = commandBusWith((command, context) -> true);

        // when
        commandBus.dispatch(commandMessage(UNSUBSCRIBED), null);

        // then
        await().atMost(Duration.ofSeconds(5)).until(() -> connector.dispatchCount.get() == 1);
        assertThat(localHandlerInvoked).isFalse();
    }

    @Test
    void routesThroughConnectorWhenPredicateDoesNotMatch() {
        // given
        CommandBus commandBus = commandBusWith((command, context) -> false);

        // when
        commandBus.dispatch(commandMessage(SUBSCRIBED), null);

        // then
        await().atMost(Duration.ofSeconds(5)).until(() -> connector.dispatchCount.get() == 1);
        assertThat(localHandlerInvoked).isFalse();
    }

    private static CommandMessage commandMessage(QualifiedName name) {
        return new GenericCommandMessage(new MessageType(name), "payload");
    }

    private static CommandResultMessage commandResultMessage(String payload) {
        return new GenericCommandResultMessage(new MessageType("io.axoniq.test.CommandResult"), payload);
    }

    /**
     * Minimal {@link CommandBusConnector} that records point-to-point command dispatches and never subscribes a remote
     * handler, standing in for a remote segment.
     */
    private static class RecordingCommandBusConnector implements CommandBusConnector {

        private final AtomicInteger dispatchCount = new AtomicInteger();

        @NonNull
        @Override
        public CompletableFuture<CommandResultMessage> dispatch(@NonNull CommandMessage command,
                                                                @Nullable ProcessingContext processingContext) {
            dispatchCount.incrementAndGet();
            return CompletableFuture.completedFuture(new GenericCommandResultMessage(command.type(), (Object) null));
        }

        @NonNull
        @Override
        public CompletableFuture<Void> subscribe(@NonNull QualifiedName commandName, int loadFactor) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean unsubscribe(@NonNull QualifiedName commandName) {
            return true;
        }

        @Override
        public void onIncomingCommand(@NonNull Handler handler) {
            // No remote inbound handling needed for this test.
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("name", "RecordingCommandBusConnector");
        }
    }
}
