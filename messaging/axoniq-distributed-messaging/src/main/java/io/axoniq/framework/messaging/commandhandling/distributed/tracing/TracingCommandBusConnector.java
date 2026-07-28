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

package io.axoniq.framework.messaging.commandhandling.distributed.tracing;

import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import org.axonframework.messaging.tracing.Span;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.SpanScope;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Type-preserving tracing decorator for {@link CommandBusConnector}.
 * <p>
 * Opens a producer span around the send leg ({@link #dispatch(CommandMessage, ProcessingContext)}) and a consumer
 * span around the receive leg ({@code onIncomingCommand} handler invocation). These connector legs nest between the
 * local bus dispatch / handle spans produced by {@code TracingCommandBus} and become observable only over a real
 * transport — in-process tests with no connector wiring will not see them.
 * <p>
 * This decorator is registered by {@code DistributedTracingConfigurationEnhancer}; it is never instantiated directly
 * by applications.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@Internal
public final class TracingCommandBusConnector implements CommandBusConnector {

    /** Prefix for the connector send-leg span ({@code "CommandBusConnector.dispatch <name>"}). */
    private static final String DISPATCH_SPAN = "CommandBusConnector.dispatch";

    /** Prefix for the connector receive-leg span ({@code "CommandBusConnector.handle <name>"}). */
    private static final String HANDLE_SPAN = "CommandBusConnector.handle";

    private final CommandBusConnector delegate;
    private final SpanFactory spanFactory;

    /**
     * Initializes a tracing {@link CommandBusConnector} wrapping the given {@code delegate}, obtaining spans from the
     * given {@code spanFactory}.
     *
     * @param delegate    the connector to delegate to
     * @param spanFactory the factory producing the tracing spans
     */
    public TracingCommandBusConnector(CommandBusConnector delegate, SpanFactory spanFactory) {
        this.delegate = Objects.requireNonNull(delegate, "delegate may not be null");
        this.spanFactory = Objects.requireNonNull(spanFactory, "spanFactory may not be null");
    }

    @Override
    public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                            @Nullable ProcessingContext processingContext) {
        Span span = spanFactory.createDispatchSpan(
                DISPATCH_SPAN + " " + command.type().qualifiedName().name(),
                command,
                processingContext
        );
        return span.branchAsync(
                processingContext,
                context -> delegate.dispatch(span.propagateContext(command), context)
        );
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor) {
        return delegate.subscribe(commandName, loadFactor);
    }

    @Override
    public boolean unsubscribe(QualifiedName commandName) {
        return delegate.unsubscribe(commandName);
    }

    @Override
    public void onIncomingCommand(Handler handler) {
        delegate.onIncomingCommand(new TracingHandler(handler, spanFactory));
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
        descriptor.describeProperty("spanFactory", spanFactory);
    }

    /**
     * Wraps the inbound {@link Handler} so the receive leg is opened as a consumer span — parented on the dispatch
     * span via the W3C trace context propagated on the command's metadata.
     */
    private static final class TracingHandler implements Handler {

        private final Handler delegate;
        private final SpanFactory spanFactory;

        private TracingHandler(Handler delegate, SpanFactory spanFactory) {
            this.delegate = delegate;
            this.spanFactory = spanFactory;
        }

        @Override
        public void handle(CommandMessage commandMessage, ResultCallback callback) {
            Span span = spanFactory.createHandlerSpan(
                    HANDLE_SPAN + " " + commandMessage.type().qualifiedName().name(),
                    commandMessage,
                    null
            );
            SpanScope scope = span.start();
            // Propagate the receive-leg span's context onto the command so the downstream bus-level handler nests
            // under this span, producing the full A → B → C → D chain over the gRPC hop.
            CommandMessage propagated = span.propagateContext(commandMessage);
            try {
                delegate.handle(propagated, new ResultCallback() {
                    @Override
                    public void onSuccess(@Nullable CommandResultMessage resultMessage) {
                        try {
                            callback.onSuccess(resultMessage);
                        } finally {
                            scope.close();
                        }
                    }

                    @Override
                    public void onError(Throwable cause) {
                        try {
                            span.recordException(cause);
                            callback.onError(cause);
                        } finally {
                            scope.close();
                        }
                    }
                });
            } catch (RuntimeException e) {
                span.recordException(e);
                scope.close();
                throw e;
            }
        }
    }
}
