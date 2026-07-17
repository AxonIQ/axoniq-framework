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

package io.axoniq.framework.messaging.queryhandling.distributed.tracing;

import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import org.axonframework.messaging.tracing.Span;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Type-preserving tracing decorator for {@link QueryBusConnector}.
 * <p>
 * Opens a producer span around the send leg ({@link #query(QueryMessage, ProcessingContext)} and
 * {@link #subscriptionQuery(QueryMessage, ProcessingContext, int)}) and a consumer span around the receive leg
 * ({@code onIncomingQuery} handler invocation). These connector legs nest between the local bus dispatch / handle
 * spans produced by {@code TracingQueryBus} and become observable only over a real transport.
 * <p>
 * This decorator is registered by {@code DistributedTracingConfigurationEnhancer}; it is never instantiated directly
 * by applications.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@Internal
public final class TracingQueryBusConnector implements QueryBusConnector {

    /** Prefix for the connector query send-leg span ({@code "QueryBusConnector.query <name>"}). */
    public static final String QUERY_SPAN = "QueryBusConnector.query";

    /** Prefix for the connector subscription-query send-leg span ({@code "QueryBusConnector.subscriptionQuery <name>"}). */
    public static final String SUBSCRIPTION_QUERY_SPAN = "QueryBusConnector.subscriptionQuery";

    /** Prefix for the connector receive-leg span ({@code "QueryBusConnector.handle <name>"}). */
    public static final String HANDLE_SPAN = "QueryBusConnector.handle";

    /** Prefix for connector subscription-query update spans ({@code "QueryBusConnector.queryUpdate <name>"}). */
    public static final String QUERY_UPDATE_SPAN = "QueryBusConnector.queryUpdate";

    private static final String MESSAGE_CONVERSATION_ID_ATTRIBUTE = "messaging.message.conversation_id";

    private final QueryBusConnector delegate;
    private final SpanFactory spanFactory;

    /**
     * Initializes a tracing {@link QueryBusConnector} wrapping the given {@code delegate}, obtaining spans from the
     * given {@code spanFactory}.
     *
     * @param delegate    the connector to delegate to
     * @param spanFactory the factory producing the tracing spans
     */
    public TracingQueryBusConnector(QueryBusConnector delegate, SpanFactory spanFactory) {
        this.delegate = Objects.requireNonNull(delegate, "delegate may not be null");
        this.spanFactory = Objects.requireNonNull(spanFactory, "spanFactory may not be null");
    }

    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query, @Nullable ProcessingContext context) {
        Span span = spanFactory.createDispatchSpan(
                QUERY_SPAN + " " + query.type().qualifiedName().name(),
                query,
                context
        );
        return span.branchStream(context, scoped -> delegate.query(span.propagateContext(query), scoped));
    }

    @Override
    public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                 @Nullable ProcessingContext context,
                                                                 int updateBufferSize) {
        Span span = spanFactory.createDispatchSpan(
                SUBSCRIPTION_QUERY_SPAN + " " + query.type().qualifiedName().name(),
                query,
                context
        ).addAttribute(MESSAGE_CONVERSATION_ID_ATTRIBUTE, query.identifier());
        // A subscription's update stream may be unbounded, so the dispatch span deliberately covers setup only.
        return span.branch(
                context,
                scoped -> delegate.subscriptionQuery(span.propagateContext(query), scoped, updateBufferSize)
                                  .onNext(entry -> {
                                      if (entry.message() instanceof SubscriptionQueryUpdateMessage update) {
                                          // Child of the emitter trace, with a link to the originating subscription.
                                          // The near-zero delivery window matches the AF4 fluxSink.next marker.
                                          // One marker per consumed entry: the subscription stream is consumed exactly
                                          // once downstream; subscribing to it again would re-fire onNext and
                                          // duplicate markers. The captured 'scoped' branch intentionally outlives its
                                          // (long-completed) context -- it is read only as the parent fallback for
                                          // updates carrying no propagated trace metadata.
                                          spanFactory.createLinkedHandlerSpan(
                                                             QUERY_UPDATE_SPAN + " "
                                                                     + update.type().qualifiedName().name(),
                                                             update,
                                                             query,
                                                             scoped
                                                     )
                                                     .addAttribute(MESSAGE_CONVERSATION_ID_ATTRIBUTE,
                                                                   query.identifier())
                                                     .branch(null, ignored -> null);
                                      }
                                  })
        );
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName name) {
        return delegate.subscribe(name);
    }

    @Override
    public boolean unsubscribe(QualifiedName name) {
        return delegate.unsubscribe(name);
    }

    @Override
    public void onIncomingQuery(Handler handler) {
        delegate.onIncomingQuery(new TracingHandler(handler, spanFactory));
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
        descriptor.describeProperty("spanFactory", spanFactory);
    }

    /**
     * Wraps the inbound {@link Handler} so the receive leg is opened as a consumer span — parented on the dispatch
     * span via the W3C trace context propagated on the query's metadata.
     */
    private static final class TracingHandler implements Handler {

        private final Handler delegate;
        private final SpanFactory spanFactory;

        private TracingHandler(Handler delegate, SpanFactory spanFactory) {
            this.delegate = delegate;
            this.spanFactory = spanFactory;
        }

        @Override
        public MessageStream<QueryResponseMessage> query(QueryMessage query) {
            Span span = spanFactory.createHandlerSpan(
                    HANDLE_SPAN + " " + query.type().qualifiedName().name(),
                    query,
                    null
            );
            // Propagate the receive-leg span's context onto the query so the downstream bus-level handler nests
            // under this span, producing the full bus-dispatch → connector-dispatch → connector-handle → bus-handle
            // chain across the gRPC hop.
            return span.branchStream(null, ignored -> delegate.query(span.propagateContext(query)));
        }

        @Override
        public Registration registerUpdateHandler(QueryMessage subscriptionQueryMessage,
                                                  UpdateCallback updateCallback) {
            return delegate.registerUpdateHandler(
                    subscriptionQueryMessage,
                    new TracingUpdateCallback(updateCallback,
                                              spanFactory,
                                              subscriptionQueryMessage.identifier())
            );
        }
    }

    /**
     * Wraps the inbound {@link UpdateCallback} so each outgoing update on the receiving side opens a dispatch span
     * for the connector update leg and propagates that context to the subscribing node.
     */
    private static final class TracingUpdateCallback implements UpdateCallback {

        private final UpdateCallback delegate;
        private final SpanFactory spanFactory;
        private final String subscriptionQueryIdentifier;

        private TracingUpdateCallback(UpdateCallback delegate,
                                      SpanFactory spanFactory,
                                      String subscriptionQueryIdentifier) {
            this.delegate = delegate;
            this.spanFactory = spanFactory;
            this.subscriptionQueryIdentifier = subscriptionQueryIdentifier;
        }

        @Override
        public CompletableFuture<Void> sendUpdate(SubscriptionQueryUpdateMessage update) {
            Span span = spanFactory.createDispatchSpan(
                    QUERY_UPDATE_SPAN + " " + update.type().qualifiedName().name(),
                    update,
                    null
            ).addAttribute(MESSAGE_CONVERSATION_ID_ATTRIBUTE, subscriptionQueryIdentifier);
            return span.branchAsync(
                    null,
                    ignored -> delegate.sendUpdate(span.propagateContext(update))
            );
        }

        @Override
        public CompletableFuture<Void> complete() {
            return delegate.complete();
        }

        @Override
        public CompletableFuture<Void> completeExceptionally(Throwable cause) {
            return delegate.completeExceptionally(cause);
        }
    }
}
