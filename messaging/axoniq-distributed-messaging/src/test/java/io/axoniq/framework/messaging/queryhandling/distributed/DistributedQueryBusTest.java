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

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.util.MockException;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryHandler;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SimpleQueryBus;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link DistributedQueryBus} verifying query routing, local handler shortcuts, and distributed query
 * behavior.
 *
 * @author Allard Buijze
 * @since 5.0.0
 */
class DistributedQueryBusTest {

    private QueryBus localSegment;
    private StubQueryBusConnector connector;
    private DistributedQueryBus testSubject;
    private DistributedQueryBusConfiguration configuration;

    /**
     * Factory method to create a query message for testing.
     *
     * @param queryName the qualified name of the query
     * @return a query message with the given name
     */
    private static QueryMessage queryMessage(QualifiedName queryName) {
        return new GenericQueryMessage(new MessageType(queryName), "test-payload");
    }

    @BeforeEach
    void setUp() {
        localSegment = new SimpleQueryBus(UnitOfWorkTestUtils.SIMPLE_FACTORY);
        connector = new StubQueryBusConnector();
        configuration = DistributedQueryBusConfiguration.DEFAULT;
    }

    @Test
    void subscribeRegistersHandlerWithConnector() {
        // Given
        testSubject = new DistributedQueryBus(localSegment, connector, configuration);
        QualifiedName queryName = new QualifiedName("TestQuery");
        QueryHandler handler = mock(QueryHandler.class);

        // When
        QueryBus result = testSubject.subscribe(queryName, handler);

        // Then
        assertThat(result).isSameAs(testSubject);
        assertThat(connector.subscribedQueries)
                .as("Connector should have been subscribed to query")
                .containsExactly(queryName);
    }

    @Test
    void queryUsesLocalHandlerWhenShortcutEnabledAndHandlerRegistered() {
        // Given
        testSubject = new DistributedQueryBus(localSegment, connector, configuration);
        QualifiedName queryName = new QualifiedName("TestQuery");

        QueryHandler handler = (query, context) -> {
            QueryResponseMessage response = mock(QueryResponseMessage.class);
            return MessageStream.fromIterable(() -> List.of(response).iterator());
        };

        QueryMessage query = queryMessage(queryName);

        // When - Subscribe handler first
        testSubject.subscribe(queryName, handler);
        testSubject.query(query, null);

        // Then - Query should use local segment (connector not invoked for query)
        assertThat(connector.queryCount.get())
                .as("Connector should not be used when local handler is available")
                .isZero();
    }

    @Test
    void queryUsesConnectorWhenNoLocalHandlerRegistered() {
        // Given
        testSubject = new DistributedQueryBus(localSegment, connector, configuration);
        QualifiedName queryName = new QualifiedName("TestQuery");
        QueryMessage query = queryMessage(queryName);

        // When - Query without registering a local handler
        testSubject.query(query, null);

        // Then - Should use connector
        assertThat(connector.queryCount.get())
                .as("Connector should be used when no local handler is registered")
                .isEqualTo(1);
    }

    @Test
    void subscriptionQueryAlwaysUsesConnector() {
        // Given
        testSubject = new DistributedQueryBus(localSegment, connector, configuration);
        QualifiedName queryName = new QualifiedName("TestQuery");
        QueryMessage query = queryMessage(queryName);

        // Register a local handler
        QueryHandler handler = (q, context) -> {
            QueryResponseMessage response = mock(QueryResponseMessage.class);
            return MessageStream.fromIterable(() -> List.of(response).iterator());
        };
        testSubject.subscribe(queryName, handler);

        // When - Subscription query even with local handler registered
        testSubject.subscriptionQuery(query, null, 10);

        // Then - Should use connector, not local segment
        assertThat(connector.subscriptionQueryCount.get())
                .as("Subscription queries should always use the connector")
                .isEqualTo(1);
    }

    @Nested
    @DisplayName("Local Query Shortcut Tests")
    class LocalQueryShortcutTests {

        @Test
        void localShortcutBypassesConnectorWhenHandlerRegistered() {
            // Given
            testSubject = new DistributedQueryBus(localSegment, connector, configuration);
            QualifiedName queryName = new QualifiedName("TestQuery");

            QueryHandler handler = (query, context) -> {
                QueryResponseMessage response = mock(QueryResponseMessage.class);
                return MessageStream.fromIterable(() -> List.of(response).iterator());
            };

            QueryMessage query = queryMessage(queryName);

            // When - Register handler and execute query
            testSubject.subscribe(queryName, handler);
            testSubject.query(query, null);

            // Then - Verify connector not used for query
            assertThat(connector.queryCount.get())
                    .as("Local shortcut should bypass connector when handler is registered")
                    .isZero();
        }

        @Test
        void queriesForDifferentHandlerStillUseConnector() {
            // Given
            testSubject = new DistributedQueryBus(localSegment, connector, configuration);
            QualifiedName registeredQueryName = new QualifiedName("RegisteredQuery");
            QualifiedName unregisteredQueryName = new QualifiedName("UnregisteredQuery");

            QueryHandler handler = (query, context) -> {
                QueryResponseMessage response = mock(QueryResponseMessage.class);
                return MessageStream.fromIterable(() -> List.of(response).iterator());
            };

            QueryMessage query = queryMessage(unregisteredQueryName);

            // When - Register handler for one query, execute different query
            testSubject.subscribe(registeredQueryName, handler);
            testSubject.query(query, null);

            // Then - Should use connector since no local handler for this query
            assertThat(connector.queryCount.get())
                    .as("Connector should be used for queries without a local handler")
                    .isEqualTo(1);
        }

        @Test
        void multipleHandlerRegistrationsAllowLocalShortcut() {
            // Given
            testSubject = new DistributedQueryBus(localSegment, connector, configuration);
            QualifiedName queryName1 = new QualifiedName("Query1");
            QualifiedName queryName2 = new QualifiedName("Query2");

            QueryHandler handler1 = (query, context) -> {
                QueryResponseMessage response = mock(QueryResponseMessage.class);
                return MessageStream.fromIterable(() -> List.of(response).iterator());
            };
            QueryHandler handler2 = (query, context) -> {
                QueryResponseMessage response = mock(QueryResponseMessage.class);
                return MessageStream.fromIterable(() -> List.of(response).iterator());
            };

            QueryMessage query1 = queryMessage(queryName1);
            QueryMessage query2 = queryMessage(queryName2);

            // When - Register multiple handlers
            testSubject.subscribe(queryName1, handler1);
            testSubject.subscribe(queryName2, handler2);

            testSubject.query(query1, null);
            testSubject.query(query2, null);

            // Then - Both should use local segment
            assertThat(connector.queryCount.get())
                    .as("Both queries should use local shortcut when handlers are registered")
                    .isZero();
        }

        @Test
        void disablingLocalShortcutForcesConnectorUsage() {
            // Given - Configuration with local shortcut disabled
            DistributedQueryBusConfiguration configWithoutShortcut =
                    configuration.preferLocalQueryHandler(false);
            testSubject = new DistributedQueryBus(localSegment, connector, configWithoutShortcut);
            QualifiedName queryName = new QualifiedName("TestQuery");

            QueryHandler handler = (query, context) -> {
                QueryResponseMessage response = mock(QueryResponseMessage.class);
                return MessageStream.fromIterable(() -> List.of(response).iterator());
            };

            QueryMessage query = queryMessage(queryName);

            // When - Register local handler and execute query
            testSubject.subscribe(queryName, handler);
            testSubject.query(query, null);

            // Then - Connector should be used despite local handler being available
            assertThat(connector.queryCount.get())
                    .as("Connector should be used when local shortcut is disabled, even if handler is registered")
                    .isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Emit and Complete And-Count Tests")
    class EmitAndCompleteAndCountTests {

        private StubUpdateCallback matchingCallbackOne;
        private StubUpdateCallback matchingCallbackTwo;
        private StubUpdateCallback nonMatchingCallback;
        private QueryMessage matchingQueryOne;
        private QueryMessage matchingQueryTwo;
        private QueryMessage nonMatchingQuery;
        private Predicate<QueryMessage> matchingFilter;

        @BeforeEach
        void registerSubscriptionQueries() {
            testSubject = new DistributedQueryBus(localSegment, connector, configuration);
            QualifiedName queryName = new QualifiedName("TestQuery");
            matchingQueryOne = queryMessage(queryName);
            matchingQueryTwo = queryMessage(queryName);
            nonMatchingQuery = queryMessage(queryName);
            matchingFilter = query -> query.identifier().equals(matchingQueryOne.identifier())
                    || query.identifier().equals(matchingQueryTwo.identifier());

            matchingCallbackOne = new StubUpdateCallback();
            matchingCallbackTwo = new StubUpdateCallback();
            nonMatchingCallback = new StubUpdateCallback();
            connector.incomingHandler.registerUpdateHandler(matchingQueryOne, matchingCallbackOne);
            connector.incomingHandler.registerUpdateHandler(matchingQueryTwo, matchingCallbackTwo);
            connector.incomingHandler.registerUpdateHandler(nonMatchingQuery, nonMatchingCallback);
        }

        @Test
        void emitUpdateAndCountReturnsNumberOfMatchingSubscriptionsAndSendsToThemOnly() {
            SubscriptionQueryUpdateMessage update =
                    new GenericSubscriptionQueryUpdateMessage(new MessageType("update"), "update-payload");

            Integer matchCount = testSubject.emitUpdateAndCount(matchingFilter, () -> update, null).join();

            assertThat(matchCount).isEqualTo(2);
            assertThat(matchingCallbackOne.sendUpdateCount.get()).isEqualTo(1);
            assertThat(matchingCallbackTwo.sendUpdateCount.get()).isEqualTo(1);
            assertThat(nonMatchingCallback.sendUpdateCount.get()).isZero();
        }

        @Test
        void emitUpdateAndCountReturnsZeroWhenNoSubscriptionsMatch() {
            SubscriptionQueryUpdateMessage update =
                    new GenericSubscriptionQueryUpdateMessage(new MessageType("update"), "update-payload");

            Integer matchCount = testSubject.emitUpdateAndCount(query -> false, () -> update, null).join();

            assertThat(matchCount).isZero();
        }

        @Test
        void emitUpdateStillSendsToAllMatchingSubscriptions() {
            SubscriptionQueryUpdateMessage update =
                    new GenericSubscriptionQueryUpdateMessage(new MessageType("update"), "update-payload");

            testSubject.emitUpdate(matchingFilter, () -> update, null).join();

            assertThat(matchingCallbackOne.sendUpdateCount.get()).isEqualTo(1);
            assertThat(matchingCallbackTwo.sendUpdateCount.get()).isEqualTo(1);
            assertThat(nonMatchingCallback.sendUpdateCount.get()).isZero();
        }

        @Test
        void completeSubscriptionsAndCountReturnsNumberOfMatchingSubscriptionsAndCompletesThemOnly() {
            Integer matchCount = testSubject.completeSubscriptionsAndCount(matchingFilter, null).join();

            assertThat(matchCount).isEqualTo(2);
            assertThat(matchingCallbackOne.completeCount.get()).isEqualTo(1);
            assertThat(matchingCallbackTwo.completeCount.get()).isEqualTo(1);
            assertThat(nonMatchingCallback.completeCount.get()).isZero();
        }

        @Test
        void completeSubscriptionsAndCountReturnsZeroWhenNoSubscriptionsMatch() {
            Integer matchCount = testSubject.completeSubscriptionsAndCount(query -> false, null).join();

            assertThat(matchCount).isZero();
        }

        @Test
        void completeSubscriptionsStillCompletesAllMatchingSubscriptions() {
            testSubject.completeSubscriptions(matchingFilter, null).join();

            assertThat(matchingCallbackOne.completeCount.get()).isEqualTo(1);
            assertThat(matchingCallbackTwo.completeCount.get()).isEqualTo(1);
            assertThat(nonMatchingCallback.completeCount.get()).isZero();
        }

        @Test
        void completeSubscriptionsExceptionallyAndCountReturnsNumberOfMatchingSubscriptionsAndCompletesThemOnly() {
            MockException cause = new MockException("Mock");

            Integer matchCount =
                    testSubject.completeSubscriptionsExceptionallyAndCount(matchingFilter, cause, null).join();

            assertThat(matchCount).isEqualTo(2);
            assertThat(matchingCallbackOne.completeExceptionallyCount.get()).isEqualTo(1);
            assertThat(matchingCallbackTwo.completeExceptionallyCount.get()).isEqualTo(1);
            assertThat(nonMatchingCallback.completeExceptionallyCount.get()).isZero();
        }

        @Test
        void completeSubscriptionsExceptionallyAndCountReturnsZeroWhenNoSubscriptionsMatch() {
            MockException cause = new MockException("Mock");

            Integer matchCount =
                    testSubject.completeSubscriptionsExceptionallyAndCount(query -> false, cause, null).join();

            assertThat(matchCount).isZero();
        }

        @Test
        void completeSubscriptionsExceptionallyStillCompletesAllMatchingSubscriptions() {
            MockException cause = new MockException("Mock");

            testSubject.completeSubscriptionsExceptionally(matchingFilter, cause, null).join();

            assertThat(matchingCallbackOne.completeExceptionallyCount.get()).isEqualTo(1);
            assertThat(matchingCallbackTwo.completeExceptionallyCount.get()).isEqualTo(1);
            assertThat(nonMatchingCallback.completeExceptionallyCount.get()).isZero();
        }
    }

    /**
     * Stub implementation of {@link QueryBusConnector} that tracks invocations for test verification.
     */
    private static class StubQueryBusConnector implements QueryBusConnector {

        final Set<QualifiedName> subscribedQueries = new HashSet<>();
        final AtomicInteger queryCount = new AtomicInteger(0);
        final AtomicInteger subscriptionQueryCount = new AtomicInteger(0);
        Handler incomingHandler;

        @NonNull
        @Override
        public MessageStream<QueryResponseMessage> query(@NonNull QueryMessage query,
                                                         @Nullable ProcessingContext context) {
            queryCount.incrementAndGet();
            QueryResponseMessage response = mock(QueryResponseMessage.class);
            return MessageStream.fromIterable(() -> List.of(response).iterator());
        }

        @NonNull
        @Override
        public MessageStream<QueryResponseMessage> subscriptionQuery(@NonNull QueryMessage query,
                                                                     @Nullable ProcessingContext context,
                                                                     int updateBufferSize) {
            subscriptionQueryCount.incrementAndGet();
            QueryResponseMessage response = mock(QueryResponseMessage.class);
            return MessageStream.fromIterable(() -> List.of(response).iterator());
        }

        @Override
        public @NonNull CompletableFuture<Void> subscribe(@NonNull QualifiedName name) {
            subscribedQueries.add(name);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean unsubscribe(@NonNull QualifiedName name) {
            return subscribedQueries.remove(name);
        }

        @Override
        public void onIncomingQuery(@NonNull Handler handler) {
            this.incomingHandler = handler;
        }

        @Override
        public void describeTo(@NonNull ComponentDescriptor descriptor) {
            descriptor.describeProperty("name", "StubQueryBusConnector");
        }
    }

    /**
     * Stub implementation of {@link QueryBusConnector.UpdateCallback} that tracks invocations for test verification.
     */
    private static class StubUpdateCallback implements QueryBusConnector.UpdateCallback {

        final AtomicInteger sendUpdateCount = new AtomicInteger(0);
        final AtomicInteger completeCount = new AtomicInteger(0);
        final AtomicInteger completeExceptionallyCount = new AtomicInteger(0);

        @Override
        public CompletableFuture<Void> sendUpdate(SubscriptionQueryUpdateMessage update) {
            sendUpdateCount.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> complete() {
            completeCount.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> completeExceptionally(Throwable cause) {
            completeExceptionallyCount.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }
}
