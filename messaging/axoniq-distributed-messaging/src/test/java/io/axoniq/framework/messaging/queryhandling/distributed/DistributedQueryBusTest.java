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
    void queryAlwaysDelegatesToConnector() {
        // The local shortcut now lives in LocalShortcutQueryBusConnector; the bus itself always uses the connector.
        testSubject = new DistributedQueryBus(localSegment, connector, configuration);
        QualifiedName queryName = new QualifiedName("TestQuery");
        QueryMessage query = queryMessage(queryName);

        // When - a local handler is registered and a query is dispatched
        testSubject.subscribe(queryName, (q, context) -> {
            QueryResponseMessage response = mock(QueryResponseMessage.class);
            return MessageStream.fromIterable(() -> List.of(response).iterator());
        });
        testSubject.query(query, null);

        // Then - the connector is used regardless of the local handler
        assertThat(connector.queryCount.get())
                .as("Bus should always delegate queries to the connector")
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
}
