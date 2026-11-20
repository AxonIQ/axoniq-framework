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

import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.AsyncInMemoryStreamableEventSource;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class TransformingEventSourceDecoratorsTest {

    private static final MessageType PARCEL_V1 = new MessageType("com.example.ParcelDispatched", "1.0.0");
    private static final MessageType PARCEL_SENT = new MessageType("com.example.ParcelSent", "1.0.0");
    private static final MessageType HEARTBEAT = new MessageType("com.example.Heartbeat", "1.0.0");

    private final EventTransformerChain chain = EventTransformerChain.builder()
                                                                     .register(EventTransformation.rename(PARCEL_V1,
                                                                                                          PARCEL_SENT))
                                                                     .register(EventTransformation.drop(HEARTBEAT))
                                                                     .build();

    @Nested
    class Streamable {

        private final AsyncInMemoryStreamableEventSource delegate = new AsyncInMemoryStreamableEventSource();
        private final TransformingStreamableEventSource testSubject = new TransformingStreamableEventSource(
                delegate, chain, neverInvokedConverter(), neverInvokedMessageTypeResolver()
        );

        @Test
        void transformsEventsReadFromTheDelegateKeepingTheirTokens() {
            // given
            delegate.publishMessage(eventOf(PARCEL_V1, "p-1"));
            delegate.publishMessage(eventOf(HEARTBEAT, "beat"));
            delegate.publishMessage(eventOf(PARCEL_SENT, "p-2"));

            // when
            MessageStream<EventMessage> stream = testSubject.open(StreamingCondition.startingFrom(null), null);

            // then
            List<MessageStream.Entry<EventMessage>> entries = new ArrayList<>();
            await().until(() -> {
                Optional<MessageStream.Entry<EventMessage>> next;
                while ((next = stream.next()).isPresent()) {
                    entries.add(next.get());
                }
                return entries.size() >= 2;
            });
            assertThat(entries).extracting(e -> e.message().type()).containsExactly(PARCEL_SENT, PARCEL_SENT);
            assertThat(entries).allSatisfy(e -> assertThat(TrackingToken.fromContext(e)).isPresent());
            stream.close();
        }

        @Test
        void widensTypeFilteringCriteriaToTheSourceTypes() {
            // given
            AtomicReference<StreamingCondition> received = new AtomicReference<>();
            TransformingStreamableEventSource recording = new TransformingStreamableEventSource(
                    new AsyncInMemoryStreamableEventSource() {
                        @Override
                        public MessageStream<EventMessage> open(StreamingCondition condition,
                                                                @Nullable ProcessingContext context) {
                            received.set(condition);
                            return super.open(condition, context);
                        }
                    }, chain, neverInvokedConverter(), neverInvokedMessageTypeResolver()
            );

            // when
            EventCriteria criteria = EventCriteria.havingAnyTag().andBeingOneOfTypes(PARCEL_SENT.qualifiedName());
            recording.open(StreamingCondition.conditionFor(TrackingToken.FIRST, criteria), null).close();

            // then
            assertThat(received.get().matches(PARCEL_V1.qualifiedName(), java.util.Set.of())).isTrue();
        }

        @Test
        void delegatesTokenLookups() {
            // when / then
            assertThat(testSubject.firstToken(null).orTimeout(5, TimeUnit.SECONDS).join())
                    .isEqualTo(delegate.firstToken(null).orTimeout(5, TimeUnit.SECONDS).join());
            assertThat(testSubject.latestToken(null).orTimeout(5, TimeUnit.SECONDS).join())
                    .isEqualTo(delegate.latestToken(null).orTimeout(5, TimeUnit.SECONDS).join());
        }
    }

    @Nested
    class Subscribable {

        private final AtomicReference<BiFunction<List<? extends EventMessage>, @Nullable ProcessingContext,
                CompletableFuture<?>>> subscriber = new AtomicReference<>();
        private final SubscribableEventSource delegate = consumer -> {
            subscriber.set(consumer);
            return () -> true;
        };
        private final TransformingSubscribableEventSource testSubject = new TransformingSubscribableEventSource(
                delegate, chain, neverInvokedConverter(), neverInvokedMessageTypeResolver()
        );

        @Test
        void transformsEveryBatchBeforeTheSubscriberReceivesIt() {
            // given
            List<EventMessage> received = new ArrayList<>();
            testSubject.subscribe((events, context) -> {
                received.addAll(events);
                return CompletableFuture.completedFuture(null);
            });

            // when
            subscriber.get().apply(List.of(eventOf(PARCEL_V1, "p-1"), eventOf(HEARTBEAT, "beat")), null)
                      .orTimeout(5, TimeUnit.SECONDS)
                      .join();

            // then
            assertThat(received).extracting(EventMessage::type).containsExactly(PARCEL_SENT);
        }

        @Test
        void doesNotCallTheSubscriberWhenEveryEventIsDropped() {
            // given
            List<List<? extends EventMessage>> batches = new ArrayList<>();
            testSubject.subscribe((events, context) -> {
                batches.add(events);
                return CompletableFuture.completedFuture(null);
            });

            // when
            CompletableFuture<?> result = subscriber.get().apply(List.of(eventOf(HEARTBEAT, "beat")), null);

            // then
            assertThat(result).isCompleted();
            assertThat(batches).isEmpty();
        }

        @Test
        void subscribingTheSameSubscriberAgainPassesTheSameSubscriberToTheDelegate() {
            // given
            BiFunction<List<? extends EventMessage>, @Nullable ProcessingContext, CompletableFuture<?>> consumer =
                    (events, context) -> CompletableFuture.completedFuture(null);
            testSubject.subscribe(consumer);
            var first = subscriber.get();

            // when
            testSubject.subscribe(consumer);

            // then - a source that accepts one subscriber only recognizes the repeated subscription
            assertThat(subscriber.get()).isSameAs(first);
        }
    }
}
