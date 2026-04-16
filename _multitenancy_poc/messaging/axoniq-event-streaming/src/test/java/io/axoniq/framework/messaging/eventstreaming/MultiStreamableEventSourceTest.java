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

package io.axoniq.framework.messaging.eventstreaming;

import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.AsyncInMemoryStreamableEventSource;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class MultiStreamableEventSourceTest {

    private MultiStreamableEventSource testSubject;
    private AsyncInMemoryStreamableEventSource eventSourceA;
    private AsyncInMemoryStreamableEventSource eventSourceB;

    @BeforeEach
    void setUp() {
        eventSourceA = new AsyncInMemoryStreamableEventSource();
        eventSourceB = new AsyncInMemoryStreamableEventSource();

        testSubject = MultiStreamableEventSource.combining("sourceA", eventSourceA)
                                                .and("sourceB", eventSourceB)
                                                .comparingTimestamps();
    }

    @Test
    void simplePublishAndConsumeFromSingleSource() {
        EventMessage publishedEvent = EventTestUtils.asEventMessage("Event1");
        eventSourceA.publishMessage(publishedEvent);

        MessageStream<EventMessage> stream = testSubject.open(
                StreamingCondition.startingFrom(null), null
        );

        assertThat(stream.hasNextAvailable()).isTrue();
        MessageStream.Entry<EventMessage> entry = stream.next().orElseThrow();
        assertThat(entry.message().payload()).isEqualTo(publishedEvent.payload());

        stream.close();
    }

    @Test
    void publishAndConsumeFromMultipleSources() {
        EventMessage event1 = EventTestUtils.asEventMessage("Event1");
        EventMessage event2 = EventTestUtils.asEventMessage("Event2");

        eventSourceA.publishMessage(event1);
        eventSourceB.publishMessage(event2);

        MessageStream<EventMessage> stream = testSubject.open(
                StreamingCondition.startingFrom(null), null
        );

        assertThat(stream.hasNextAvailable()).isTrue();

        List<String> payloads = new ArrayList<>();
        stream.next().ifPresent(e -> payloads.add((String) e.message().payload()));
        stream.next().ifPresent(e -> payloads.add((String) e.message().payload()));

        assertThat(payloads)
                .hasSize(2)
                .contains("Event1")
                .contains("Event2");

        stream.close();
    }

    @Test
    void messagesAreOrderedByTimestampByDefault() throws InterruptedException {
        EventMessage event1 = EventTestUtils.asEventMessage("Event1");
        eventSourceA.publishMessage(event1);

        // Ensure event2 has a later timestamp
        Thread.sleep(10);

        EventMessage event2 = EventTestUtils.asEventMessage("Event2");
        eventSourceB.publishMessage(event2);

        MessageStream<EventMessage> stream = testSubject.open(
                StreamingCondition.startingFrom(null), null
        );

        MessageStream.Entry<EventMessage> first = stream.next().orElseThrow();
        MessageStream.Entry<EventMessage> second = stream.next().orElseThrow();

        assertThat(first.message().payload()).isEqualTo("Event1");
        assertThat(second.message().payload()).isEqualTo("Event2");

        stream.close();
    }

    @Test
    void customComparatorIsUsed() {
        // Create a comparator that prioritizes sourceB
        Comparator<MessageStream.Entry<EventMessage>> customComparator =
                Comparator.comparing((MessageStream.Entry<EventMessage> entry) ->
                                             !Objects.toString(entry.message().payload()).contains("B"))
                          .thenComparing(entry -> entry.message().timestamp());

        testSubject = MultiStreamableEventSource.combining("sourceA", eventSourceA)
                                                .and("sourceB", eventSourceB)
                                                .comparingUsing(customComparator);

        eventSourceA.publishMessage(EventTestUtils.asEventMessage("EventA"));
        eventSourceB.publishMessage(EventTestUtils.asEventMessage("EventB"));

        MessageStream<EventMessage> stream = testSubject.open(
                StreamingCondition.startingFrom(null), null
        );

        MessageStream.Entry<EventMessage> first = stream.next().orElseThrow();

        // EventB should come first due to custom comparator
        assertThat(first.message().payload()).isEqualTo("EventB");

        stream.close();
    }

    @Test
    void peekDoesNotConsumeMessage() {
        EventMessage event = EventTestUtils.asEventMessage("Event1");
        eventSourceA.publishMessage(event);

        MessageStream<EventMessage> stream = testSubject.open(
                StreamingCondition.startingFrom(null), null
        );

        MessageStream.Entry<EventMessage> peeked = stream.peek().orElseThrow();
        assertThat(peeked.message().payload()).isEqualTo("Event1");

        // Message should still be available
        assertThat(stream.hasNextAvailable()).isTrue();

        MessageStream.Entry<EventMessage> consumed = stream.next().orElseThrow();
        assertThat(consumed.message().payload()).isEqualTo("Event1");

        stream.close();
    }

    @Test
    void firstTokenReturnsMultiSourceToken() {
        TrackingToken token = testSubject.firstToken(null).join();

        assertThat(token).isInstanceOf(MultiSourceTrackingToken.class);
        MultiSourceTrackingToken multiToken = (MultiSourceTrackingToken) token;

        assertThat(multiToken.getTokenForStream("sourceA")).isNotNull();
        assertThat(multiToken.getTokenForStream("sourceB")).isNotNull();
    }

    @Test
    void latestTokenReturnsMultiSourceToken() {
        eventSourceA.publishMessage(EventTestUtils.asEventMessage("Event1"));
        eventSourceB.publishMessage(EventTestUtils.asEventMessage("Event2"));

        TrackingToken token = testSubject.latestToken(null).join();

        assertThat(token).isInstanceOf(MultiSourceTrackingToken.class);
        MultiSourceTrackingToken multiToken = (MultiSourceTrackingToken) token;

        assertThat(multiToken.getTokenForStream("sourceA")).isNotNull();
        assertThat(multiToken.getTokenForStream("sourceB")).isNotNull();
    }

    @Test
    void tokenAtReturnsMultiSourceToken() {
        Instant now = Instant.now();
        eventSourceA.publishMessage(EventTestUtils.asEventMessage("Event1"));
        eventSourceB.publishMessage(EventTestUtils.asEventMessage("Event2"));

        TrackingToken token = testSubject.tokenAt(now, null).join();

        assertThat(token).isInstanceOf(MultiSourceTrackingToken.class);
        MultiSourceTrackingToken multiToken = (MultiSourceTrackingToken) token;

        assertThat(multiToken.getTokenForStream("sourceA")).isNotNull();
        assertThat(multiToken.getTokenForStream("sourceB")).isNotNull();
    }

    @Test
    void openWithNullTokenStartsFromBeginning() {
        eventSourceA.publishMessage(EventTestUtils.asEventMessage("Event1"));
        eventSourceB.publishMessage(EventTestUtils.asEventMessage("Event2"));

        MessageStream<EventMessage> stream = testSubject.open(
                StreamingCondition.startingFrom(null), null
        );

        assertThat(stream.hasNextAvailable()).isTrue();
        assertThat(stream.next()).isPresent();

        stream.close();
    }

    @Test
    void openWithMultiSourceTokenStartsFromToken() {
        // Create a token manually representing position after the first event in each source
        Map<String, TrackingToken> tokenMap = new HashMap<>();
        tokenMap.put("sourceA", new GlobalSequenceTrackingToken(1));
        tokenMap.put("sourceB", new GlobalSequenceTrackingToken(1));
        MultiSourceTrackingToken token = new MultiSourceTrackingToken(tokenMap);

        // Publish events - positions 0, 1, 2
        eventSourceA.publishMessage(EventTestUtils.asEventMessage("Event1")); // position 0
        eventSourceA.publishMessage(EventTestUtils.asEventMessage("Event2")); // position 1
        eventSourceA.publishMessage(EventTestUtils.asEventMessage("Event3")); // position 2

        // Open stream with token at position 1 - should only see Event3 (position 2)
        MessageStream<EventMessage> stream = testSubject.open(
                StreamingCondition.startingFrom(token), null
        );

        // Should see Event3
        assertThat(stream.hasNextAvailable()).isTrue();
        MessageStream.Entry<EventMessage> entry = stream.next().orElseThrow();
        assertThat(entry.message().payload()).isEqualTo("Event3");

        stream.close();
    }

    @Test
    void openWithIncompatibleTokenThrowsException() {
        GlobalSequenceTrackingToken incompatibleToken = new GlobalSequenceTrackingToken(0);

        assertThatThrownBy(() -> testSubject.open(StreamingCondition.startingFrom(incompatibleToken), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void trackingTokenIsUpdatedAsMessagesAreConsumed() {
        eventSourceA.publishMessage(EventTestUtils.asEventMessage("Event1"));
        eventSourceB.publishMessage(EventTestUtils.asEventMessage("Event2"));

        MessageStream<EventMessage> stream = testSubject.open(
                StreamingCondition.startingFrom(null), null
        );

        MessageStream.Entry<EventMessage> entry1 = stream.next().orElseThrow();
        TrackingToken token1 = TrackingToken.fromContext(entry1).orElseThrow();

        assertThat(token1).isInstanceOf(MultiSourceTrackingToken.class);

        MessageStream.Entry<EventMessage> entry2 = stream.next().orElseThrow();
        TrackingToken token2 = TrackingToken.fromContext(entry2).orElseThrow();

        assertThat(token2).isInstanceOf(MultiSourceTrackingToken.class);
        assertThat(token1).isNotEqualTo(token2);

        stream.close();
    }

    @Test
    void streamCompletesWhenAllSourcesComplete() {
        eventSourceA.publishMessage(EventTestUtils.asEventMessage("Event1"));

        MessageStream<EventMessage> stream = testSubject.open(
                StreamingCondition.startingFrom(null), null
        );

        stream.next();

        // Close source streams to complete them
        assertThat(stream.isCompleted()).isFalse();

        stream.close();
    }

    @Test
    void callbackIsInvokedWhenMessagesAreAvailable() {
        AtomicBoolean callbackInvoked = new AtomicBoolean(false);

        MessageStream<EventMessage> stream = testSubject.open(
                StreamingCondition.startingFrom(null), null
        );

        stream.setCallback(() -> callbackInvoked.set(true));

        eventSourceA.publishMessage(EventTestUtils.asEventMessage("Event1"));
        eventSourceA.runOnAvailableCallback();

        assertThat(callbackInvoked.get()).isTrue();

        stream.close();
    }

    @Test
    void builderRejectsNonUniqueSourceIds() {
        assertThatThrownBy(() -> MultiStreamableEventSource.combining("source", eventSourceA)
                                                             .and("source", eventSourceB)
                                                             .comparingTimestamps()
        ).isInstanceOf(Exception.class);
    }

    @Test
    void streamsAreClosedWhenOneSourceFailsToOpen() {
        AtomicBoolean streamClosed = new AtomicBoolean(false);
        AtomicBoolean streamOpened = new AtomicBoolean(false);
        StreamableEventSource source1 = new AsyncInMemoryStreamableEventSource() {
            @Override
            public @NonNull MessageStream<EventMessage> open(@NonNull StreamingCondition condition,
                                                             @Nullable ProcessingContext context) {
                if (streamOpened.compareAndSet(false, true)) {
                    return super.open(condition, context).onClose(() -> streamClosed.set(true));
                }
                throw new RuntimeException("Simulating failure in second stream");
            }
        };
        StreamableEventSource source2 = new AsyncInMemoryStreamableEventSource() {
            @Override
            public @NonNull MessageStream<EventMessage> open(@NonNull StreamingCondition condition,
                                                             @Nullable ProcessingContext context) {
                if (streamOpened.compareAndSet(false, true)) {
                    return super.open(condition, context).onClose(() -> streamClosed.set(true));
                }
                throw new RuntimeException("Simulating failure in second stream");
            }
        };

        testSubject = MultiStreamableEventSource.combining("source1", source1)
                                                .and("source2", source2)
                                                .comparingTimestamps();

        // Create a MultiSourceTrackingToken with both source tokens
        Map<String, TrackingToken> tokenMap = new HashMap<>();
        tokenMap.put("source1", new GlobalSequenceTrackingToken(0));
        tokenMap.put("source2", new GlobalSequenceTrackingToken(0));
        MultiSourceTrackingToken token = new MultiSourceTrackingToken(tokenMap);

        // Opening should throw when source2 fails
        assertThatThrownBy(() -> testSubject.open(StreamingCondition.startingFrom(token), null))
                .isInstanceOf(RuntimeException.class);

        // Verify that source1's stream was closed due to the failure
        assertThat(streamClosed.get()).as("Stream from source1 should be closed when source2 fails to open").isTrue();
    }

    @Test
    void emptyStreamReturnsNoMessages() {
        MessageStream<EventMessage> stream = testSubject.open(
                StreamingCondition.startingFrom(null), null
        );

        assertThat(stream.hasNextAvailable()).isFalse();
        assertThat(stream.next()).isEmpty();

        stream.close();
    }

    @Test
    void multipleStreamsCanBeOpenedConcurrently() {
        eventSourceA.publishMessage(EventTestUtils.asEventMessage("Event1"));
        eventSourceB.publishMessage(EventTestUtils.asEventMessage("Event2"));

        MessageStream<EventMessage> stream1 = testSubject.open(
                StreamingCondition.startingFrom(null), null
        );
        MessageStream<EventMessage> stream2 = testSubject.open(
                StreamingCondition.startingFrom(null), null
        );

        assertThat(stream1.next()).isPresent();
        assertThat(stream2.next()).isPresent();

        stream1.close();
        stream2.close();
    }

    @Nested
    class BuilderApiTest {

        @Test
        void combiningWithTimestampComparison() {
            // given
            StreamableEventSource source1 = mock(StreamableEventSource.class);
            StreamableEventSource source2 = mock(StreamableEventSource.class);

            // when
            MultiStreamableEventSource result = MultiStreamableEventSource
                    .combining("source1", source1)
                    .and("source2", source2)
                    .comparingTimestamps();

            // then
            assertThat(result).isNotNull();
            assertThat(result.sources())
                    .hasSize(2)
                    .containsEntry("source1", source1)
                    .containsEntry("source2", source2);
        }

        @Test
        void combiningWithCustomComparator() {
            // given
            StreamableEventSource source1 = mock(StreamableEventSource.class);
            @SuppressWarnings("unchecked")
            Comparator<MessageStream.Entry<EventMessage>> comparator = mock(Comparator.class);

            // when
            MultiStreamableEventSource result = MultiStreamableEventSource
                    .combining("source1", source1)
                    .comparingUsing(comparator);

            // then
            assertThat(result).isNotNull();
            assertThat(result.sources())
                    .hasSize(1)
                    .containsEntry("source1", source1);
        }

        @Test
        void combiningSingleSource() {
            // given
            StreamableEventSource source1 = mock(StreamableEventSource.class);

            // when
            MultiStreamableEventSource result = MultiStreamableEventSource
                    .combining("source1", source1)
                    .comparingTimestamps();

            // then
            assertThat(result).isNotNull();
            assertThat(result.sources())
                    .hasSize(1)
                    .containsEntry("source1", source1);
        }

        @Test
        void combiningMultipleSources() {
            // given
            StreamableEventSource source1 = mock(StreamableEventSource.class);
            StreamableEventSource source2 = mock(StreamableEventSource.class);
            StreamableEventSource source3 = mock(StreamableEventSource.class);

            // when
            MultiStreamableEventSource result = MultiStreamableEventSource
                    .combining("source1", source1)
                    .and("source2", source2)
                    .and("source3", source3)
                    .comparingTimestamps();

            // then
            assertThat(result).isNotNull();
            assertThat(result.sources())
                    .hasSize(3)
                    .containsEntry("source1", source1)
                    .containsEntry("source2", source2)
                    .containsEntry("source3", source3);
        }

        @Test
        void combiningRejectsDuplicateSourceNames() {
            // given
            StreamableEventSource source1 = mock(StreamableEventSource.class);
            StreamableEventSource source2 = mock(StreamableEventSource.class);

            // when/then
            assertThatThrownBy(() -> MultiStreamableEventSource.combining("source1", source1)
                                                               .and("source1", source2) // duplicate name
                                                               .comparingTimestamps()
            ).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void combiningRejectsNullSourceName() {
            // when/then
            assertThatThrownBy(() -> MultiStreamableEventSource.combining(null, mock(StreamableEventSource.class))
            ).isInstanceOf(NullPointerException.class);
        }

        @Test
        void combiningRejectsNullSource() {
            // when/then
            assertThatThrownBy(() -> MultiStreamableEventSource.combining("source1", null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void andRejectsNullSourceName() {
            // given
            StreamableEventSource source1 = mock(StreamableEventSource.class);

            // when/then
            assertThatThrownBy(() -> MultiStreamableEventSource
                    .combining("source1", source1)
                    .and(null, mock(StreamableEventSource.class))
            ).isInstanceOf(NullPointerException.class);
        }

        @Test
        void andRejectsNullSource() {
            // given
            StreamableEventSource source1 = mock(StreamableEventSource.class);

            // when/then
            assertThatThrownBy(() -> MultiStreamableEventSource
                    .combining("source1", source1)
                    .and("source2", null)
            ).isInstanceOf(NullPointerException.class);
        }

        @Test
        void comparingUsingRejectsNullComparator() {
            // given
            StreamableEventSource source1 = mock(StreamableEventSource.class);

            // when/then
            assertThatThrownBy(() -> MultiStreamableEventSource
                    .combining("source1", source1)
                    .comparingUsing(null)
            ).isInstanceOf(NullPointerException.class);
        }

        @Test
        void sourcesReturnsUnmodifiableMap() {
            // given
            StreamableEventSource source1 = mock(StreamableEventSource.class);
            MultiStreamableEventSource result = MultiStreamableEventSource
                    .combining("source1", source1)
                    .comparingTimestamps();

            // when/then
            assertThatThrownBy(() -> result.sources().put("newSource", mock(StreamableEventSource.class)))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
