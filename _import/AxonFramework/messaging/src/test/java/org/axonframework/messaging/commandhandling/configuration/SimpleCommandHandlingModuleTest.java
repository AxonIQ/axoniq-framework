/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.messaging.commandhandling.configuration;

import org.axonframework.common.configuration.StubLifecycleRegistry;
import org.axonframework.messaging.commandhandling.CommandHandlingComponent;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.interception.InterceptingCommandHandlingComponent;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link CommandHandlingModule}.
 */
class SimpleCommandHandlingModuleTest {

    private static final QualifiedName COMMAND_NAME = new QualifiedName(String.class);

    private CommandHandlingModule.SetupPhase setupPhase;
    private CommandHandlingModule.CommandHandlerPhase commandHandlerPhase;

    @BeforeEach
    void setUp() {
        setupPhase = CommandHandlingModule.named("test-subject");
        commandHandlerPhase = setupPhase.commandHandlers();
    }

    @Test
    void nameReturnsModuleName() {
        assertEquals("test-subject", setupPhase.commandHandlers().build().name());
    }

    @Test
    void buildAnnotatedCommandHandlingComponentSucceedsAndRegisters() {
        //noinspection unused
        var myCommandHandlingObject = new Object() {
            @org.axonframework.messaging.commandhandling.annotation.CommandHandler
            public String handle(String command) {
                return command;
            }
        };

        var resultConfig =
                setupPhase.commandHandlers()
                          .autodetectedCommandHandlingComponent(c -> myCommandHandlingObject)
                          .build()
                          .build(MessagingConfigurer.create().build(), new StubLifecycleRegistry());

        Optional<CommandHandlingComponent> optionalHandlingComponent = resultConfig.getOptionalComponent(
                CommandHandlingComponent.class, "CommandHandlingComponent[test-subject]");
        assertTrue(optionalHandlingComponent.isPresent());
        assertTrue(optionalHandlingComponent.get().supportedCommands().contains(COMMAND_NAME));
    }

    @Test
    void namedThrowsNullPointerExceptionForNullModuleName() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> CommandHandlingModule.named(null));
    }

    @Test
    void commandHandlerThrowsNullPointerExceptionForNullCommandName() {
        //noinspection DataFlowIssue
        assertThrows(
                NullPointerException.class,
                () -> commandHandlerPhase.commandHandler(null, (cmd, ctx) -> MessageStream.empty().cast())
        );
    }

    @Test
    void commandHandlerThrowsNullPointerExceptionForNullCommandHandler() {
        //noinspection DataFlowIssue
        assertThrows(
                NullPointerException.class,
                () -> commandHandlerPhase.commandHandler(
                        COMMAND_NAME,
                        (org.axonframework.messaging.commandhandling.CommandHandler) null
                )
        );
    }

    @Test
    void commandHandlingComponentThrowsNullPointerExceptionForNullBuilder() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> commandHandlerPhase.commandHandlingComponent(null));
    }

    @Test
    void autodetectedCommandHandlingComponentThrowsNullPointerExceptionForNullBuilder() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> commandHandlerPhase.autodetectedCommandHandlingComponent(null));
    }

    @Test
    void commandHandlersConsumerThrowsNullPointerExceptionForNullConsumer() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> setupPhase.commandHandlers(null));
    }

    @Nested
    class InterceptedTest {

        private static final GenericCommandMessage SAMPLE_COMMAND =
                new GenericCommandMessage(new MessageType(String.class), "payload");

        @Test
        void singleInterceptorIsInvokedBeforeHandler() {
            // given
            List<String> invocationLog = new ArrayList<>();
            var component = buildModule(commandHandlerPhase
                    .commandHandler(COMMAND_NAME, (cmd, ctx) -> {
                        invocationLog.add("handler");
                        return MessageStream.empty().cast();
                    })
                    .intercepted(cfg -> (msg, ctx, chain) -> {
                        invocationLog.add("interceptor");
                        return chain.proceed(msg, ctx);
                    }));

            // when
            component.handle(SAMPLE_COMMAND, StubProcessingContext.forMessage(SAMPLE_COMMAND)).asCompletableFuture()
                     .orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(invocationLog).containsExactly("interceptor", "handler");
        }

        @Test
        void multipleInterceptorsAreInvokedInRegistrationOrder() {
            // given
            List<String> invocationLog = new ArrayList<>();
            var component = buildModule(commandHandlerPhase
                    .commandHandler(COMMAND_NAME, (cmd, ctx) -> {
                        invocationLog.add("handler");
                        return MessageStream.empty().cast();
                    })
                    .intercepted(cfg -> (msg, ctx, chain) -> {
                        invocationLog.add("first");
                        return chain.proceed(msg, ctx);
                    })
                    .intercepted(cfg -> (msg, ctx, chain) -> {
                        invocationLog.add("second");
                        return chain.proceed(msg, ctx);
                    }));

            // when
            component.handle(SAMPLE_COMMAND, StubProcessingContext.forMessage(SAMPLE_COMMAND)).asCompletableFuture()
                     .orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(invocationLog).containsExactly("first", "second", "handler");
        }

        @Test
        void noWrappingWhenNoInterceptorsRegistered() {
            // given / when
            var component = buildModule(commandHandlerPhase
                    .commandHandler(COMMAND_NAME, (cmd, ctx) -> MessageStream.empty()));

            // then
            assertThat(component).isNotInstanceOf(InterceptingCommandHandlingComponent.class);
        }

        @Test
        void wrappingOccursWhenInterceptorIsRegistered() {
            // given / when
            var component = buildModule(commandHandlerPhase
                    .commandHandler(COMMAND_NAME, (cmd, ctx) -> MessageStream.empty())
                    .intercepted(cfg -> (msg, ctx, chain) -> chain.proceed(msg, ctx)));

            // then
            assertThat(component).isInstanceOf(InterceptingCommandHandlingComponent.class);
        }

        @Test
        void interceptorCanShortCircuitHandling() {
            // given
            List<String> invocationLog = new ArrayList<>();
            var component = buildModule(commandHandlerPhase
                    .commandHandler(COMMAND_NAME, (cmd, ctx) -> {
                        invocationLog.add("handler");
                        return MessageStream.empty().cast();
                    })
                    .intercepted(cfg -> (msg, ctx, chain) -> MessageStream.failed(new RuntimeException("access denied"))));

            // when / then
            assertThat(component.handle(SAMPLE_COMMAND, StubProcessingContext.forMessage(SAMPLE_COMMAND))
                                .asCompletableFuture())
                    .failsWithin(5, java.util.concurrent.TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(RuntimeException.class)
                    .withMessageContaining("access denied");

            assertThat(invocationLog).isEmpty();
        }

        @Test
        void interceptorNullThrowsNullPointerException() {
            // given / when / then
            assertThrows(NullPointerException.class, () -> commandHandlerPhase.intercepted(null));
        }

        private CommandHandlingComponent buildModule(CommandHandlingModule.CommandHandlerPhase phase) {
            var config = phase.build()
                              .build(MessagingConfigurer.create().build(), new StubLifecycleRegistry());
            return config.getComponent(CommandHandlingComponent.class, "CommandHandlingComponent[test-subject]");
        }
    }
}
