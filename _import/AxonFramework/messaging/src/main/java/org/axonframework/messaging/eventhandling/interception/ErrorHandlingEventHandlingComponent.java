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

package org.axonframework.messaging.eventhandling.interception;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.DelegatingEventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventHandlingExceptionHandler;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Objects;

/**
 * An {@link EventHandlingComponent} decorator that applies an {@link EventHandlingExceptionHandler} to every event
 * handler invocation. When the delegate's {@link #handle(EventMessage, ProcessingContext)} returns a stream that
 * completes with an error, the exception handler is called.
 * <p>
 * If the exception handler returns normally the error is suppressed and the stream completes successfully. If the
 * exception handler throws, that exception propagates instead.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
public class ErrorHandlingEventHandlingComponent extends DelegatingEventHandlingComponent {

    private final EventHandlingExceptionHandler exceptionHandler;

    /**
     * Constructs a decorator that applies the given {@code exceptionHandler} to the given {@code delegate}.
     *
     * @param delegate         the component to delegate event handling to
     * @param exceptionHandler the handler to invoke when an exception is thrown
     */
    public ErrorHandlingEventHandlingComponent(EventHandlingComponent delegate,
                                               EventHandlingExceptionHandler exceptionHandler) {
        super(delegate);
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler, "The exception handler must not be null.");
    }

    @Override
    public MessageStream.Empty<Message> handle(EventMessage event, ProcessingContext context) {
        return delegate.handle(event, context)
                       .onErrorContinue(error -> {
                           try {
                               return exceptionHandler.handle(event, context, error);
                           } catch (Exception e) {
                               return MessageStream.failed(e);
                           }
                       })
                       .ignoreEntries()
                       .cast();
    }
}
