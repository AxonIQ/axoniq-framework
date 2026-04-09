/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.framework.messaging.queryhandling.distributed;

import org.assertj.core.api.Assertions;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryHandler;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SimpleQueryBus;
import org.junit.jupiter.api.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

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

    /**
     * Stub implementation of {@link QueryBusConnector} that tracks invocations for test verification.
     */
    private static class StubQueryBusConnector implements QueryBusConnector {

        final Set<QualifiedName> subscribedQueries = new HashSet<>();
        final AtomicInteger queryCount = new AtomicInteger(0);
        final AtomicInteger subscriptionQueryCount = new AtomicInteger(0);

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
            // No-op for tests
        }

        @Override
        public void describeTo(@NonNull ComponentDescriptor descriptor) {
            descriptor.describeProperty("name", "StubQueryBusConnector");
        }
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
}
