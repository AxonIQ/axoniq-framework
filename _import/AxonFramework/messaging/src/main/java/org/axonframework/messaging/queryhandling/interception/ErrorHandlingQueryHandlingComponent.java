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

package org.axonframework.messaging.queryhandling.interception;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.DelegatingQueryHandlingComponent;
import org.axonframework.messaging.queryhandling.QueryHandlingComponent;
import org.axonframework.messaging.queryhandling.QueryHandlingExceptionHandler;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;

import java.util.Objects;

/**
 * A {@link QueryHandlingComponent} decorator that applies a {@link QueryHandlingExceptionHandler} to every query
 * handler invocation. When the delegate's {@link #handle(QueryMessage, ProcessingContext)} returns a stream that
 * completes with an error, the exception handler is called.
 * <p>
 * If the exception handler returns an empty stream, the error is suppressed and the stream completes with no results.
 * If the exception handler returns a non-empty stream, those results substitute the error. If the exception handler
 * throws, that exception propagates instead.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
public class ErrorHandlingQueryHandlingComponent extends DelegatingQueryHandlingComponent {

    private final QueryHandlingExceptionHandler exceptionHandler;

    /**
     * Constructs a decorator that applies the given {@code exceptionHandler} to the given {@code delegate}.
     *
     * @param delegate         the component to delegate query handling to
     * @param exceptionHandler the handler to invoke when an exception is thrown
     */
    public ErrorHandlingQueryHandlingComponent(QueryHandlingComponent delegate,
                                               QueryHandlingExceptionHandler exceptionHandler) {
        super(delegate);
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler, "The exception handler must not be null.");
    }

    @Override
    public MessageStream<QueryResponseMessage> handle(QueryMessage query, ProcessingContext context) {
        return delegate.handle(query, context)
                       .onErrorContinue(error -> {
                           try {
                               return exceptionHandler.handle(query, context, error);
                           } catch (Exception e) {
                               return MessageStream.failed(e);
                           }
                       });
    }
}
