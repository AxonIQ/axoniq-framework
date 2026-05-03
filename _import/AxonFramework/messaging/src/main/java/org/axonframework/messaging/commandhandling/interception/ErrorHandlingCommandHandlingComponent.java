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

package org.axonframework.messaging.commandhandling.interception;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.commandhandling.CommandHandlingComponent;
import org.axonframework.messaging.commandhandling.CommandHandlingExceptionHandler;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.DelegatingCommandHandlingComponent;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Objects;

/**
 * A {@link CommandHandlingComponent} decorator that applies a {@link CommandHandlingExceptionHandler} to every command
 * handler invocation. When the delegate's {@link #handle(CommandMessage, ProcessingContext)} returns a stream that
 * completes with an error, the exception handler is called.
 * <p>
 * If the exception handler returns an empty stream, the error is suppressed and the stream completes with no result.
 * If the exception handler returns a non-empty stream, those results are used as the command result. If the exception
 * handler throws, that exception propagates instead.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
public class ErrorHandlingCommandHandlingComponent extends DelegatingCommandHandlingComponent {

    private final CommandHandlingExceptionHandler exceptionHandler;

    /**
     * Constructs a decorator that applies the given {@code exceptionHandler} to the given {@code delegate}.
     *
     * @param delegate         the component to delegate command handling to
     * @param exceptionHandler the handler to invoke when an exception is thrown
     */
    public ErrorHandlingCommandHandlingComponent(CommandHandlingComponent delegate,
                                                 CommandHandlingExceptionHandler exceptionHandler) {
        super(delegate);
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler, "The exception handler must not be null.");
    }

    @Override
    public MessageStream.Single<CommandResultMessage> handle(CommandMessage command, ProcessingContext context) {
        return delegate.handle(command, context)
                       .onErrorContinue(error -> {
                           try {
                               return exceptionHandler.handle(command, context, error);
                           } catch (Exception e) {
                               return MessageStream.failed(e);
                           }
                       })
                       .first();
    }
}
