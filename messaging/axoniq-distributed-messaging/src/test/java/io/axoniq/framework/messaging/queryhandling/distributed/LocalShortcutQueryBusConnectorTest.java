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

package io.axoniq.framework.messaging.queryhandling.distributed;

import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link LocalShortcutQueryBusConnector}.
 *
 * @author Allard Buijze
 */
class LocalShortcutQueryBusConnectorTest {

    private final RecordingQueryBusConnector delegate = new RecordingQueryBusConnector();
    private final RecordingHandler localHandler = new RecordingHandler();

    private QueryMessage query;
    private QualifiedName queryName;

    @BeforeEach
    void setUp() {
        query = asQueryMessage("query");
        queryName = query.type().qualifiedName();
    }

    private LocalShortcutQueryBusConnector connectorFor(LocalQueryDispatchPredicate predicate) {
        return new LocalShortcutQueryBusConnector(delegate, predicate);
    }

    @Nested
    class DirectQuery {

        @Test
        void queriesLocalHandlerWhenSubscribedAndPredicateMatches() {
            LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> true);
            connector.onIncomingQuery(localHandler);
            connector.subscribe(queryName);

            connector.query(query, null);

            assertThat(localHandler.queryCount).hasValue(1);
            assertThat(delegate.queryCount).hasValue(0);
        }

        @Test
        void routesThroughDelegateWhenPredicateDoesNotMatch() {
            LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> false);
            connector.onIncomingQuery(localHandler);
            connector.subscribe(queryName);

            connector.query(query, null);

            assertThat(delegate.queryCount).hasValue(1);
            assertThat(localHandler.queryCount).hasValue(0);
        }

        @Test
        void routesThroughDelegateWhenQueryNotLocallySubscribed() {
            LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> true);
            connector.onIncomingQuery(localHandler);
            // No matching subscription registered.

            connector.query(query, null);

            assertThat(delegate.queryCount).hasValue(1);
            assertThat(localHandler.queryCount).hasValue(0);
        }

        @Test
        void routesThroughDelegateWhenNoLocalHandlerRegistered() {
            LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> true);
            connector.subscribe(queryName);
            // onIncomingQuery never called, so no local handler is available yet.

            connector.query(query, null);

            assertThat(delegate.queryCount).hasValue(1);
        }

        @Test
        void unsubscribeStopsLocalDispatch() {
            LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> true);
            connector.onIncomingQuery(localHandler);
            connector.subscribe(queryName);
            connector.unsubscribe(queryName);

            connector.query(query, null);

            assertThat(delegate.queryCount).hasValue(1);
            assertThat(localHandler.queryCount).hasValue(0);
            assertThat(delegate.unsubscribed).containsExactly(queryName);
        }

        @Test
        void passesProcessingContextToPredicate() {
            AtomicReference<Object> seenContext = new AtomicReference<>("unset");
            LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> {
                seenContext.set(ctx);
                return false;
            });
            connector.onIncomingQuery(localHandler);
            connector.subscribe(queryName);

            connector.query(query, null);

            assertThat(seenContext.get()).isNull();
        }
    }

    @Nested
    class SubscriptionQuery {

        @Test
        void alwaysRoutesThroughDelegateEvenWhenSubscribedAndPredicateMatches() {
            LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> true);
            connector.onIncomingQuery(localHandler);
            connector.subscribe(queryName);

            connector.subscriptionQuery(query, null, 42);

            assertThat(delegate.subscriptionQueryCount).hasValue(1);
            assertThat(localHandler.queryCount).hasValue(0);
        }
    }

    @Nested
    class Delegation {

        @Test
        void subscribeOnIncomingQueryAndUnsubscribeDelegateToWrappedConnector() {
            LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> false);

            connector.subscribe(queryName);
            connector.onIncomingQuery(localHandler);
            boolean unsubscribed = connector.unsubscribe(queryName);

            assertThat(delegate.subscribed).containsExactly(queryName);
            assertThat(delegate.incomingHandler).isSameAs(localHandler);
            assertThat(delegate.unsubscribed).containsExactly(queryName);
            assertThat(unsubscribed).isTrue();
        }

        @Test
        void describeToDescribesWrapperOfDelegate() {
            LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> false);
            RecordingComponentDescriptor descriptor = new RecordingComponentDescriptor();

            connector.describeTo(descriptor);

            assertThat(descriptor.properties).containsEntry("delegate", delegate);
        }
    }

    private QueryMessage asQueryMessage(String payload) {
        return new GenericQueryMessage(MessageType.fromString("querymessage#1.0"), payload);
    }

    /**
     * Recording {@link QueryBusConnector} that counts dispatches and captures subscriptions, standing in for the
     * wrapped connector.
     */
    private static class RecordingQueryBusConnector implements QueryBusConnector {

        private final AtomicInteger queryCount = new AtomicInteger();
        private final AtomicInteger subscriptionQueryCount = new AtomicInteger();
        private final List<QualifiedName> subscribed = new ArrayList<>();
        private final List<QualifiedName> unsubscribed = new ArrayList<>();
        private Handler incomingHandler;

        @Override
        public MessageStream<QueryResponseMessage> query(QueryMessage query, @Nullable ProcessingContext context) {
            queryCount.incrementAndGet();
            return MessageStream.empty().cast();
        }

        @Override
        public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                     @Nullable ProcessingContext context,
                                                                     int updateBufferSize) {
            subscriptionQueryCount.incrementAndGet();
            return MessageStream.empty().cast();
        }

        @Override
        public CompletableFuture<Void> subscribe(QualifiedName name) {
            subscribed.add(name);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean unsubscribe(QualifiedName name) {
            unsubscribed.add(name);
            return true;
        }

        @Override
        public void onIncomingQuery(Handler handler) {
            this.incomingHandler = handler;
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("name", "RecordingQueryBusConnector");
        }
    }

    /**
     * Recording local {@link QueryBusConnector.Handler} that counts the queries handed to it for local handling.
     */
    private static class RecordingHandler implements QueryBusConnector.Handler {

        private final AtomicInteger queryCount = new AtomicInteger();

        @Override
        public MessageStream<QueryResponseMessage> query(QueryMessage query) {
            queryCount.incrementAndGet();
            return MessageStream.empty().cast();
        }

        @Override
        public Registration registerUpdateHandler(QueryMessage subscriptionQueryMessage,
                                                  QueryBusConnector.UpdateCallback updateCallback) {
            return () -> true;
        }
    }

    /**
     * Recording {@link ComponentDescriptor} capturing the properties described to it, so the wrapper relationship can
     * be asserted without mocking.
     */
    private static class RecordingComponentDescriptor implements ComponentDescriptor {

        private final Map<String, Object> properties = new HashMap<>();

        @Override
        public void describeProperty(String name, @Nullable Object object) {
            properties.put(name, object);
        }

        @Override
        public void describeProperty(String name, @Nullable Collection<?> collection) {
            properties.put(name, collection);
        }

        @Override
        public void describeProperty(String name, @Nullable Map<?, ?> map) {
            properties.put(name, map);
        }

        @Override
        public void describeProperty(String name, @Nullable String value) {
            properties.put(name, value);
        }

        @Override
        public void describeProperty(String name, @Nullable Long value) {
            properties.put(name, value);
        }

        @Override
        public void describeProperty(String name, @Nullable Boolean value) {
            properties.put(name, value);
        }
    }
}
