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

package io.axoniq.framework.messaging.transformation.events;

import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Read-side decoration adds transformation behaviour only to {@code source(...)} and
 * {@code open(...)}. Every other {@link EventStore} / {@link EventStoreTransaction} method
 * MUST delegate to the inner instance unchanged: the chain runs at read time only, so
 * append, token and subscription operations must not be decorated. This test pins each
 * delegation so a future refactor cannot silently introduce a side effect.
 */
final class TransformingEventStoreDelegationTest {

    private static final MessageType V1 = new MessageType("com.example.Sample", "1.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();
    private static final MessageTypeResolver RESOLVER = alwaysEmptyMessageTypeResolver();

    @Nested
    final class EventStoreDelegations {

        @Test
        void publishDelegatesUnchanged() {
            EventStore delegate = Mockito.mock(EventStore.class);
            CompletableFuture<Void> expected = CompletableFuture.completedFuture(null);
            EventMessage event = new GenericEventMessage(V1, "payload");
            ProcessingContext context = new StubProcessingContext();
            when(delegate.publish(context, List.of(event))).thenReturn(expected);
            TransformingEventStore decorated = newDecorator(delegate);

            CompletableFuture<Void> actual = decorated.publish(context, List.of(event));

            assertThat(actual).isSameAs(expected);
            verify(delegate).publish(context, List.of(event));
        }

        @Test
        void firstTokenDelegatesUnchanged() {
            EventStore delegate = Mockito.mock(EventStore.class);
            ProcessingContext context = new StubProcessingContext();
            CompletableFuture<TrackingToken> expected = CompletableFuture.completedFuture(TrackingToken.FIRST);
            when(delegate.firstToken(context)).thenReturn(expected);

            assertThat(newDecorator(delegate).firstToken(context)).isSameAs(expected);
            verify(delegate).firstToken(context);
        }

        @Test
        void latestTokenDelegatesUnchanged() {
            EventStore delegate = Mockito.mock(EventStore.class);
            ProcessingContext context = new StubProcessingContext();
            CompletableFuture<TrackingToken> expected = CompletableFuture.completedFuture(TrackingToken.LATEST);
            when(delegate.latestToken(context)).thenReturn(expected);

            assertThat(newDecorator(delegate).latestToken(context)).isSameAs(expected);
            verify(delegate).latestToken(context);
        }

        @Test
        void tokenAtDelegatesUnchanged() {
            EventStore delegate = Mockito.mock(EventStore.class);
            ProcessingContext context = new StubProcessingContext();
            Instant at = Instant.parse("2026-01-15T10:00:00Z");
            CompletableFuture<TrackingToken> expected = CompletableFuture.completedFuture(TrackingToken.FIRST);
            when(delegate.tokenAt(at, context)).thenReturn(expected);

            assertThat(newDecorator(delegate).tokenAt(at, context)).isSameAs(expected);
            verify(delegate).tokenAt(at, context);
        }

        @Test
        void subscribeDelegatesUnchanged() {
            EventStore delegate = Mockito.mock(EventStore.class);
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> consumer =
                    (events, context) -> CompletableFuture.completedFuture(null);
            org.axonframework.common.Registration expected = () -> true;
            when(delegate.subscribe(consumer)).thenReturn(expected);

            assertThat(newDecorator(delegate).subscribe(consumer)).isSameAs(expected);
            verify(delegate).subscribe(consumer);
        }

        @Test
        void transactionCachesPerProcessingContextSoSuccessiveCallsReturnTheSameWrappingInstance() {
            EventStore delegate = Mockito.mock(EventStore.class);
            EventStoreTransaction innerTransaction = Mockito.mock(EventStoreTransaction.class);
            when(delegate.transaction(Mockito.any())).thenReturn(innerTransaction);
            TransformingEventStore decorated = newDecorator(delegate);
            ProcessingContext context = new StubProcessingContext();

            EventStoreTransaction first = decorated.transaction(context);
            EventStoreTransaction second = decorated.transaction(context);

            assertThat(first)
                    .as("successive transaction(ctx) calls on the same context must reuse the cached wrapper")
                    .isSameAs(second);
        }
    }

    @Nested
    final class EventStoreTransactionDelegations {

        @Test
        void appendEventDelegatesUnchanged() {
            EventStoreTransaction innerTransaction = Mockito.mock(EventStoreTransaction.class);
            EventMessage event = new GenericEventMessage(V1, "payload");

            newTransaction(innerTransaction).appendEvent(event);

            verify(innerTransaction).appendEvent(event);
        }

        @Test
        void onAppendDelegatesUnchanged() {
            EventStoreTransaction innerTransaction = Mockito.mock(EventStoreTransaction.class);
            Consumer<EventMessage> callback = event -> {
            };

            newTransaction(innerTransaction).onAppend(callback);

            verify(innerTransaction).onAppend(callback);
        }

        @Test
        void overrideAppendConditionDelegatesUnchanged() {
            EventStoreTransaction innerTransaction = Mockito.mock(EventStoreTransaction.class);
            UnaryOperator<AppendCondition> override = condition -> condition;

            newTransaction(innerTransaction).overrideAppendCondition(override);

            verify(innerTransaction).overrideAppendCondition(override);
        }

        @Test
        void appendPositionDelegatesUnchanged() {
            EventStoreTransaction innerTransaction = Mockito.mock(EventStoreTransaction.class);
            ConsistencyMarker expected = Mockito.mock(ConsistencyMarker.class);
            doReturn(expected).when(innerTransaction).appendPosition();

            assertThat(newTransaction(innerTransaction).appendPosition()).isSameAs(expected);
            verify(innerTransaction).appendPosition();
        }
    }

    private static TransformingEventStore newDecorator(EventStore delegate) {
        EventTransformerChain chain = EventTransformerChain.builder().build();
        return new TransformingEventStore(delegate, chain, CONVERTER, RESOLVER);
    }

    private static TransformingEventStoreTransaction newTransaction(EventStoreTransaction delegate) {
        EventTransformerChain chain = EventTransformerChain.builder().build();
        return new TransformingEventStoreTransaction(
                delegate, chain, new StubProcessingContext(), CONVERTER, RESOLVER);
    }
}
