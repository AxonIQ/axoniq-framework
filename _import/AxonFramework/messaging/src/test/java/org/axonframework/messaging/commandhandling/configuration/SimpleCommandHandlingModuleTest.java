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

import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.StubLifecycleRegistry;
import org.axonframework.messaging.commandhandling.CommandHandlingComponent;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link CommandHandlingModule}.
 */
class SimpleCommandHandlingModuleTest {

    private static final QualifiedName COMMAND_NAME = new QualifiedName("test-command");
    private static final GenericCommandMessage SAMPLE_COMMAND =
            new GenericCommandMessage(new MessageType("test-command"), "payload");
    private static final StubProcessingContext STUB_PROCESSING_CONTEXT = new StubProcessingContext();

    private static CommandHandlingComponent buildComponent(CommandHandlingModule.CommandHandlerPhase phase) {
        var moduleName = "test";
        Configuration config = phase.build()
                                    .build(MessagingConfigurer.create().build(), new StubLifecycleRegistry());
        return config.getOptionalComponent(CommandHandlingComponent.class,
                                           "CommandHandlingComponent[" + moduleName + "]")
                     .orElseThrow();
    }

    @Nested
    class WithExceptionHandlerTest {

        @Test
        void exceptionHandlerIsInvokedWhenHandlerThrows() {
            // given
            List<String> invocationLog = new ArrayList<>();
            var component = buildComponent(
                    CommandHandlingModule.named("test")
                                        .commandHandlers()
                                        .commandHandler(COMMAND_NAME,
                                                        (cmd, ctx) -> MessageStream.failed(new RuntimeException("handler failed")))
                                        .withExceptionHandler((cmd, ctx, error) -> {
                                            invocationLog.add("exceptionHandler");
                                            return MessageStream.empty();
                                        })
            );

            // when
            component.handle(SAMPLE_COMMAND, STUB_PROCESSING_CONTEXT);

            // then
            assertThat(invocationLog).containsExactly("exceptionHandler");
        }

        @Test
        void exceptionHandlerCanSuppressException() {
            // given
            var component = buildComponent(
                    CommandHandlingModule.named("test")
                                        .commandHandlers()
                                        .commandHandler(COMMAND_NAME,
                                                        (cmd, ctx) -> MessageStream.failed(new RuntimeException("handler failed")))
                                        .withExceptionHandler((cmd, ctx, error) -> MessageStream.empty())
            );

            // when - exception handler returns empty, suppressing the error
            var result = component.handle(SAMPLE_COMMAND, STUB_PROCESSING_CONTEXT);
            result.peek(); // force lazy evaluation of the onErrorContinue chain

            // then
            assertThat(result.error()).isEmpty();
        }

        @Test
        void exceptionHandlerCanPropagateError() {
            // given
            var component = buildComponent(
                    CommandHandlingModule.named("test")
                                        .commandHandlers()
                                        .commandHandler(COMMAND_NAME,
                                                        (cmd, ctx) -> MessageStream.failed(new RuntimeException("original")))
                                        .withExceptionHandler((cmd, ctx, error) -> MessageStream.failed(new IOException("wrapped")))
            );

            // when - exception handler returns a failed stream, the error propagates
            var result = component.handle(SAMPLE_COMMAND, STUB_PROCESSING_CONTEXT);
            result.peek(); // force lazy evaluation of the onErrorContinue chain

            // then
            assertThat(result.error()).isPresent();
            assertThat(result.error().get()).isInstanceOf(IOException.class).hasMessage("wrapped");
        }

        @Test
        void exceptionHandlerCanSubstituteResult() {
            // given
            var substituteResult = new GenericCommandResultMessage(new MessageType("result"), "substitute");
            var component = buildComponent(
                    CommandHandlingModule.named("test")
                                        .commandHandlers()
                                        .commandHandler(COMMAND_NAME,
                                                        (cmd, ctx) -> MessageStream.failed(new RuntimeException("handler failed")))
                                        .withExceptionHandler((cmd, ctx, error) -> MessageStream.just(substituteResult))
            );

            // when - exception handler returns a result message to substitute
            var result = component.handle(SAMPLE_COMMAND, STUB_PROCESSING_CONTEXT);

            // then - asCompletableFuture() forces evaluation and returns the substituted result
            assertThat(result.asCompletableFuture().orTimeout(1, java.util.concurrent.TimeUnit.SECONDS).join())
                    .isNotNull()
                    .satisfies(entry -> assertThat(entry.message()).isEqualTo(substituteResult));
        }

        @Test
        void exceptionHandlerUnexpectedThrowIsWrappedInFailedStream() {
            // given
            var component = buildComponent(
                    CommandHandlingModule.named("test")
                                        .commandHandlers()
                                        .commandHandler(COMMAND_NAME,
                                                        (cmd, ctx) -> MessageStream.failed(new RuntimeException("original")))
                                        .withExceptionHandler((cmd, ctx, error) -> {
                                            throw new RuntimeException("unexpected");
                                        })
            );

            // when - exception handler throws unexpectedly, the thrown exception propagates as a failed stream
            var result = component.handle(SAMPLE_COMMAND, STUB_PROCESSING_CONTEXT);
            result.peek(); // force lazy evaluation of the onErrorContinue chain

            // then
            assertThat(result.error()).isPresent();
            assertThat(result.error().get()).isInstanceOf(RuntimeException.class).hasMessage("unexpected");
        }

        @Test
        void firstRegisteredHandlerSeesExceptionFirst() {
            // given
            List<String> invocationLog = new ArrayList<>();
            var component = buildComponent(
                    CommandHandlingModule.named("test")
                                        .commandHandlers()
                                        .commandHandler(COMMAND_NAME,
                                                        (cmd, ctx) -> MessageStream.failed(new RuntimeException("handler failed")))
                                        .withExceptionHandler((cmd, ctx, error) -> {
                                            // first registered: logs and propagates so second can also run
                                            invocationLog.add("first");
                                            return MessageStream.failed(error);
                                        })
                                        .withExceptionHandler((cmd, ctx, error) -> {
                                            invocationLog.add("second");
                                            return MessageStream.empty();
                                        })
            );

            // when
            component.handle(SAMPLE_COMMAND, STUB_PROCESSING_CONTEXT);

            // then - first registered handler runs first
            assertThat(invocationLog).containsExactly("first", "second");
        }
    }
}
