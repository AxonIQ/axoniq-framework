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
import org.axonframework.messaging.tracing.support.TestSpanFactory;
import org.axonframework.messaging.tracing.support.TestSpanFactory.TestSpanType;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.axonframework.messaging.tracing.Span;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class TracingQueryBusConnectorTest {

    private static final String QUERY_SPAN = "QueryBusConnector.query MyQuery";
    private static final String SUBSCRIPTION_QUERY_SPAN = "QueryBusConnector.subscriptionQuery MyQuery";
    private static final String HANDLE_SPAN = "QueryBusConnector.handle MyQuery";
    private static final String QUERY_UPDATE_SPAN = "QueryBusConnector.queryUpdate MyUpdate";
    private static final String EMITTER_SPAN = "Emitter";
    private static final String MESSAGE_CONVERSATION_ID_ATTRIBUTE = "messaging.message.conversation_id";

    private TestSpanFactory spanFactory;
    private RecordingConnector delegate;
    private TracingQueryBusConnector testSubject;

    private final QueryMessage query =
            new GenericQueryMessage(new MessageType("MyQuery"), "the-payload");
    private final QueryResponseMessage response =
            new GenericQueryResponseMessage(new MessageType("Result"), "result-payload");
    private final SubscriptionQueryUpdateMessage update =
            new GenericSubscriptionQueryUpdateMessage(new MessageType("MyUpdate"), "update-payload");

    @BeforeEach
    void setUp() {
        spanFactory = new TestSpanFactory();
        delegate = new RecordingConnector();
        testSubject = new TracingQueryBusConnector(delegate, spanFactory);
    }

    @Nested
    class SendLeg {

        @Test
        void querySendSpanStaysActiveUntilTheResultStreamTerminates() {
            // given
            delegate.queryResult = MessageStream.just(response);

            // when
            MessageStream<QueryResponseMessage> result = testSubject.query(query, null);

            // then
            spanFactory.verifySpanActive(QUERY_SPAN);
            assertThat(result.next()).isPresent();
            assertThat(result.next()).isEmpty();
            spanFactory.verifySpanCompleted(QUERY_SPAN);
            spanFactory.verifySpanHasType(QUERY_SPAN, TestSpanType.DISPATCH);
            spanFactory.verifySpanPropagated(QUERY_SPAN, query);
        }

        @Test
        void opensADispatchSpanAroundTheSubscriptionQuerySendLeg() {
            // given
            delegate.subscriptionQueryResult = MessageStream.just(response);

            // when
            testSubject.subscriptionQuery(query, null, 16);

            // then
            spanFactory.verifySpanCompleted(SUBSCRIPTION_QUERY_SPAN);
            spanFactory.verifySpanHasType(SUBSCRIPTION_QUERY_SPAN, TestSpanType.DISPATCH);
            spanFactory.verifySpanPropagated(SUBSCRIPTION_QUERY_SPAN, query);
            spanFactory.verifySpanHasAttributeValue(SUBSCRIPTION_QUERY_SPAN,
                                                    MESSAGE_CONVERSATION_ID_ATTRIBUTE,
                                                    query.identifier());
        }

        @Test
        void createsALinkedDeliverySpanForUpdatesButNotForTheInitialResult() {
            // given an update carrying the emitter's propagated trace context
            Span emitter = spanFactory.createDispatchSpan(EMITTER_SPAN, update, null);
            emitter.branch(null, ignored -> {
                emitter.propagateContext(update);
                return null;
            });
            delegate.subscriptionQueryResult = MessageStream.fromItems(response, update);

            // when the mixed result stream is consumed
            MessageStream<QueryResponseMessage> result = testSubject.subscriptionQuery(query, null, 16);
            assertThat(result.next()).get().extracting(MessageStream.Entry::message).isSameAs(response);

            // then the initial result opens no update span under ANY name -- the prefix check catches a broken
            // update-type guard, which would open a span suffixed with the initial result's own type instead
            spanFactory.verifyNoSpanWithNamePrefix(TracingQueryBusConnector.QUERY_UPDATE_SPAN);

            // when the update is delivered
            assertThat(result.next()).get().extracting(MessageStream.Entry::message).isSameAs(update);
            assertThat(result.next()).isEmpty();

            // then exactly one delivery marker exists, parented on the emitter and linked to the originating query.
            // The link resolves to the CONNECTOR's subscriptionQuery span here because TestSpanFactory's
            // identity-keyed propagation makes this connector's own propagateContext(query) the last writer; in
            // production, Micrometer's copy-based propagation leaves the original query instance carrying the
            // BUS-level subscription span's context, so the link targets that span instead (as the AxonServer
            // integration test proves). Both follow the same mechanism: link = the query's propagated context.
            spanFactory.verifySpanCompleted(QUERY_UPDATE_SPAN, update);
            spanFactory.verifySpanCount(QUERY_UPDATE_SPAN, 1);
            spanFactory.verifySpanHasType(QUERY_UPDATE_SPAN, TestSpanType.LINKED_HANDLER);
            spanFactory.verifySpanHasParent(QUERY_UPDATE_SPAN, EMITTER_SPAN);
            spanFactory.verifySpanHasLink(QUERY_UPDATE_SPAN, SUBSCRIPTION_QUERY_SPAN);
            spanFactory.verifySpanHasAttributeValue(QUERY_UPDATE_SPAN,
                                                    MESSAGE_CONVERSATION_ID_ATTRIBUTE,
                                                    query.identifier());
        }

        @Test
        void opensExactlyOneDeliveryMarkerPerUpdateEntry() {
            // given a subscription stream delivering two updates of the same type
            SubscriptionQueryUpdateMessage secondUpdate =
                    new GenericSubscriptionQueryUpdateMessage(new MessageType("MyUpdate"), "second-update-payload");
            delegate.subscriptionQueryResult = MessageStream.<QueryResponseMessage>fromItems(update, secondUpdate);

            // when both updates are consumed
            MessageStream<QueryResponseMessage> result = testSubject.subscriptionQuery(query, null, 16);
            assertThat(result.next()).isPresent();
            assertThat(result.next()).isPresent();
            assertThat(result.next()).isEmpty();

            // then each update entry produced exactly one delivery marker -- neither zero-for-N nor duplicates
            spanFactory.verifySpanCount(QUERY_UPDATE_SPAN, 2);
        }
    }

    @Nested
    class ReceiveLeg {

        @Test
        void queryHandleSpanStaysActiveUntilTheResultStreamTerminates() {
            // given an inbound handler registered through the tracing connector
            testSubject.onIncomingQuery(new QueryBusConnector.Handler() {
                @Override
                public MessageStream<QueryResponseMessage> query(QueryMessage q) {
                    return MessageStream.just(response);
                }

                @Override
                public Registration registerUpdateHandler(QueryMessage subscriptionQueryMessage,
                                                          QueryBusConnector.UpdateCallback updateCallback) {
                    return () -> true;
                }
            });

            // when the underlying connector delivers an inbound query
            MessageStream<QueryResponseMessage> result = delegate.handler.query(query);

            // then the handle span covers consumption, not just stream construction
            spanFactory.verifySpanActive(HANDLE_SPAN);
            assertThat(result.next()).isPresent();
            assertThat(result.next()).isEmpty();
            spanFactory.verifySpanCompleted(HANDLE_SPAN);
            spanFactory.verifySpanHasType(HANDLE_SPAN, TestSpanType.HANDLER);
        }

        @Test
        void propagatesTheHandleSpanContextToTheDownstreamQuery() {
            // given a handler capturing the query it receives from the tracing wrapper
            CapturingHandler capturingHandler = new CapturingHandler();
            testSubject.onIncomingQuery(capturingHandler);

            // when
            delegate.handler.query(query);

            // then the downstream bus-level handler receives the query with the receive-leg span's context
            // propagated onto it, so the bus handle span nests under the connector handle span
            spanFactory.verifySpanPropagated(HANDLE_SPAN, capturingHandler.received);
        }

        @Test
        void tracesAndPropagatesEachEmittedSubscriptionQueryUpdate() {
            // given an update callback registered through the tracing receive leg
            CapturingHandler handler = new CapturingHandler();
            RecordingUpdateCallback transportCallback = new RecordingUpdateCallback();
            testSubject.onIncomingQuery(handler);
            delegate.handler.registerUpdateHandler(query, transportCallback);

            // when the local handler emits an update
            handler.registeredUpdateCallback.sendUpdate(update).orTimeout(5, TimeUnit.SECONDS).join();

            // then the connector dispatch span feeds the remote delivery parent
            assertThat(transportCallback.receivedUpdate).isSameAs(update);
            spanFactory.verifySpanCompleted(QUERY_UPDATE_SPAN, update);
            spanFactory.verifySpanHasType(QUERY_UPDATE_SPAN, TestSpanType.DISPATCH);
            spanFactory.verifySpanPropagated(QUERY_UPDATE_SPAN, update);
            spanFactory.verifySpanHasAttributeValue(QUERY_UPDATE_SPAN,
                                                    MESSAGE_CONVERSATION_ID_ATTRIBUTE,
                                                    query.identifier());

            // when a second update of the same type is emitted
            SubscriptionQueryUpdateMessage secondUpdate =
                    new GenericSubscriptionQueryUpdateMessage(new MessageType("MyUpdate"), "second-update-payload");
            handler.registeredUpdateCallback.sendUpdate(secondUpdate).orTimeout(5, TimeUnit.SECONDS).join();

            // then each emission opened exactly one dispatch span and propagated onto its own update
            spanFactory.verifySpanCount(QUERY_UPDATE_SPAN, 2);
            spanFactory.verifySpanPropagated(QUERY_UPDATE_SPAN, secondUpdate);
        }
    }

    @Nested
    class Delegation {

        @Test
        void forwardsSubscribeAndUnsubscribeToTheDelegate() {
            // when
            testSubject.subscribe(new QualifiedName("MyQuery"));
            boolean unsubscribed = testSubject.unsubscribe(new QualifiedName("MyQuery"));

            // then
            assertThat(delegate.subscribed).isEqualTo(new QualifiedName("MyQuery"));
            assertThat(unsubscribed).isTrue();
        }
    }

    private static final class CapturingHandler implements QueryBusConnector.Handler {

        private QueryMessage received;
        private QueryBusConnector.UpdateCallback registeredUpdateCallback;

        @Override
        public MessageStream<QueryResponseMessage> query(QueryMessage q) {
            this.received = q;
            return MessageStream.empty().cast();
        }

        @Override
        public Registration registerUpdateHandler(QueryMessage subscriptionQueryMessage,
                                                  QueryBusConnector.UpdateCallback updateCallback) {
            this.registeredUpdateCallback = updateCallback;
            return () -> true;
        }
    }

    private static final class RecordingUpdateCallback implements QueryBusConnector.UpdateCallback {

        private SubscriptionQueryUpdateMessage receivedUpdate;

        @Override
        public CompletableFuture<Void> sendUpdate(SubscriptionQueryUpdateMessage update) {
            this.receivedUpdate = update;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> complete() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> completeExceptionally(Throwable cause) {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class RecordingConnector implements QueryBusConnector {

        private Handler handler;
        private QualifiedName subscribed;
        private MessageStream<QueryResponseMessage> queryResult = MessageStream.empty().cast();
        private MessageStream<QueryResponseMessage> subscriptionQueryResult = MessageStream.empty().cast();

        @Override
        public MessageStream<QueryResponseMessage> query(QueryMessage query, @Nullable ProcessingContext context) {
            return queryResult;
        }

        @Override
        public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                     @Nullable ProcessingContext context,
                                                                     int updateBufferSize) {
            return subscriptionQueryResult;
        }

        @Override
        public CompletableFuture<Void> subscribe(QualifiedName name) {
            this.subscribed = name;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean unsubscribe(QualifiedName name) {
            return true;
        }

        @Override
        public void onIncomingQuery(Handler handler) {
            this.handler = handler;
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
        }
    }
}
