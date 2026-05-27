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

package io.axoniq.framework.tracing.messaging.internal;

import io.axoniq.framework.tracing.ProcessingContextSpanBinding;
import io.axoniq.framework.tracing.Span;
import io.axoniq.framework.tracing.SpanFactory;
import io.axoniq.framework.tracing.SpanNames;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandHandler;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Delegating {@link CommandBus} decorator that opens a tracing span around command dispatch and command handling.
 * <p>
 * On {@link #dispatch(CommandMessage, ProcessingContext) dispatch} a dispatch span is opened, the active tracing
 * context is propagated onto the command's metadata (so a remote handler can continue the trace), and the span is
 * ended when the dispatch future completes. Each subscribed {@link CommandHandler} is wrapped so that handling opens a
 * handler span — parented on the dispatch span via the propagated context — bound to the handling
 * {@link ProcessingContext}'s lifecycle.
 * <p>
 * This decorator is registered by {@code MessagingTracingConfigurationEnhancer}; it is never instantiated directly by
 * applications.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@Internal
public final class TracingCommandBus implements CommandBus {

    private final CommandBus delegate;
    private final SpanFactory spanFactory;

    /**
     * Initializes a tracing {@link CommandBus} wrapping the given {@code delegate}, obtaining spans from the given
     * {@code spanFactory}.
     *
     * @param delegate    the command bus to delegate to
     * @param spanFactory the factory producing the tracing spans
     */
    public TracingCommandBus(CommandBus delegate, SpanFactory spanFactory) {
        this.delegate = Objects.requireNonNull(delegate, "delegate may not be null");
        this.spanFactory = Objects.requireNonNull(spanFactory, "spanFactory may not be null");
    }

    @Override
    public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                            @Nullable ProcessingContext processingContext) {
        Span span = spanFactory.createDispatchSpan(SpanNames.commandDispatch(command), command, processingContext);
        return span.runSupplierAsync(
                () -> delegate.dispatch(span.propagateContext(command), processingContext)
        );
    }

    @Override
    public CommandBus subscribe(QualifiedName name, CommandHandler commandHandler) {
        delegate.subscribe(name, new TracingCommandHandler(commandHandler, spanFactory));
        return this;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
        descriptor.describeProperty("spanFactory", spanFactory);
    }

    /**
     * Wraps a {@link CommandHandler} to open a handler span around its invocation, bound to the handling context's
     * lifecycle.
     */
    private static final class TracingCommandHandler implements CommandHandler {

        private final CommandHandler delegate;
        private final SpanFactory spanFactory;

        private TracingCommandHandler(CommandHandler delegate, SpanFactory spanFactory) {
            this.delegate = delegate;
            this.spanFactory = spanFactory;
        }

        @Override
        public MessageStream.Single<CommandResultMessage> handle(CommandMessage command, ProcessingContext context) {
            Span span = spanFactory.createHandlerSpan(SpanNames.commandHandle(command), command, context);
            ProcessingContextSpanBinding.bind(span, context);
            return delegate.handle(command, context);
        }
    }
}
