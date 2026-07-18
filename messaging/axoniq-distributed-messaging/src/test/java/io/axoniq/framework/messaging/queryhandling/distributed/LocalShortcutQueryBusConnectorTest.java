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
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.junit.jupiter.api.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link LocalShortcutQueryBusConnector}.
 *
 * @author Allard Buijze
 */
class LocalShortcutQueryBusConnectorTest {

    private final QueryBusConnector delegate = mock(QueryBusConnector.class);
    private final QueryBusConnector.Handler localHandler = mock(QueryBusConnector.Handler.class);
    private final ComponentDescriptor componentDescriptor = mock(ComponentDescriptor.class);

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

    @Test
    void queriesLocalHandlerWhenSubscribedAndPredicateMatches() {
        LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> true);
        connector.onIncomingQuery(localHandler);
        connector.subscribe(queryName);

        MessageStream<QueryResponseMessage> handlerStream = anyStream();
        when(localHandler.query(query)).thenReturn(handlerStream);

        MessageStream<QueryResponseMessage> result = connector.query(query, null);

        assertThat(result).isSameAs(handlerStream);
        verify(localHandler).query(query);
        verify(delegate, never()).query(any(), any());
    }

    @Test
    void routesThroughDelegateWhenPredicateDoesNotMatch() {
        LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> false);
        connector.onIncomingQuery(localHandler);
        connector.subscribe(queryName);

        MessageStream<QueryResponseMessage> expected = anyStream();
        when(delegate.query(query, null)).thenReturn(expected);

        MessageStream<QueryResponseMessage> result = connector.query(query, null);

        assertThat(result).isSameAs(expected);
        verify(delegate).query(query, null);
        verify(localHandler, never()).query(any());
    }

    @Test
    void routesThroughDelegateWhenQueryNotLocallySubscribed() {
        LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> true);
        connector.onIncomingQuery(localHandler);
        // No matching subscription registered.

        MessageStream<QueryResponseMessage> expected = anyStream();
        when(delegate.query(query, null)).thenReturn(expected);

        MessageStream<QueryResponseMessage> result = connector.query(query, null);

        assertThat(result).isSameAs(expected);
        verify(delegate).query(query, null);
        verify(localHandler, never()).query(any());
    }

    @Test
    void routesThroughDelegateWhenNoLocalHandlerRegistered() {
        LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> true);
        connector.subscribe(queryName);
        // onIncomingQuery never called, so no local handler is available yet.

        MessageStream<QueryResponseMessage> expected = anyStream();
        when(delegate.query(query, null)).thenReturn(expected);

        MessageStream<QueryResponseMessage> result = connector.query(query, null);

        assertThat(result).isSameAs(expected);
        verify(delegate).query(query, null);
    }

    @Test
    void unsubscribeStopsLocalDispatch() {
        LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> true);
        connector.onIncomingQuery(localHandler);
        connector.subscribe(queryName);
        connector.unsubscribe(queryName);

        MessageStream<QueryResponseMessage> expected = anyStream();
        when(delegate.query(query, null)).thenReturn(expected);

        MessageStream<QueryResponseMessage> result = connector.query(query, null);

        assertThat(result).isSameAs(expected);
        verify(delegate).unsubscribe(queryName);
        verify(localHandler, never()).query(any());
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
        when(delegate.query(any(), any())).thenReturn(anyStream());

        connector.query(query, null);

        assertThat(seenContext.get()).isNull();
    }

    @Test
    void subscriptionQueryAlwaysRoutesThroughDelegate() {
        LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> true);
        connector.onIncomingQuery(localHandler);
        connector.subscribe(queryName);

        MessageStream<QueryResponseMessage> expected = anyStream();
        when(delegate.subscriptionQuery(query, null, 42)).thenReturn(expected);

        MessageStream<QueryResponseMessage> result = connector.subscriptionQuery(query, null, 42);

        assertThat(result).isSameAs(expected);
        verify(delegate).subscriptionQuery(query, null, 42);
        verify(localHandler, never()).query(any());
    }

    @Test
    void subscribeAndOnIncomingQueryDelegateToWrappedConnector() {
        LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> false);
        when(delegate.subscribe(queryName)).thenReturn(CompletableFuture.completedFuture(null));
        when(delegate.unsubscribe(queryName)).thenReturn(true);

        connector.subscribe(queryName);
        connector.onIncomingQuery(localHandler);
        boolean unsubscribed = connector.unsubscribe(queryName);

        verify(delegate).subscribe(queryName);
        verify(delegate).onIncomingQuery(localHandler);
        verify(delegate).unsubscribe(queryName);
        assertThat(unsubscribed).isTrue();
    }

    @Test
    void describeToDescribesWrapperOfDelegate() {
        LocalShortcutQueryBusConnector connector = connectorFor((q, ctx) -> false);

        connector.describeTo(componentDescriptor);

        verify(componentDescriptor).describeWrapperOf(delegate);
    }

    @SuppressWarnings("unchecked")
    private static MessageStream<QueryResponseMessage> anyStream() {
        return mock(MessageStream.class);
    }

    private QueryMessage asQueryMessage(String payload) {
        return new GenericQueryMessage(MessageType.fromString("querymessage#1.0"), payload);
    }
}
