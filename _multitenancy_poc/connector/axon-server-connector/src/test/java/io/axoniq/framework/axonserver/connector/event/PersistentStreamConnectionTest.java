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

package io.axoniq.framework.axonserver.connector.event;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.PersistentStream;
import io.axoniq.axonserver.connector.event.PersistentStreamCallbacks;
import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.axonserver.connector.event.PersistentStreamSegment;
import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.streams.PersistentStreamEvent;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link PersistentStreamConnection}.
 */
class PersistentStreamConnectionTest {

    private static final String STREAM_NAME = "stream-name";
    private static final String STREAM_ID = "stream-id";

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final PersistentStreamProperties properties =
            new PersistentStreamProperties(STREAM_NAME, 2, "Seq", Collections.emptyList(), "0", null);
    private final Map<String, MockPersistentStream> mockPersistentStreams = new ConcurrentHashMap<>();

    private PersistentStreamConnection testSubject;

    @BeforeEach
    void setup() {
        AxonServerConnectionManager mockConnectionManager = mock(AxonServerConnectionManager.class);
        AxonServerConnection mockConnection = mock(AxonServerConnection.class);
        EventChannel mockEventChannel = mock(EventChannel.class);

        when(mockEventChannel.openPersistentStream(anyString(), anyInt(), anyInt(), any(), any()))
                .thenAnswer(invocationOnMock -> {
                    String streamId = invocationOnMock.getArgument(0);
                    mockPersistentStreams.put(streamId,
                                              new MockPersistentStream(invocationOnMock.getArgument(3)));
                    return mockPersistentStreams.get(streamId);
                });
        when(mockConnection.eventChannel()).thenReturn(mockEventChannel);
        when(mockConnectionManager.getConnection(anyString())).thenReturn(mockConnection);

        testSubject = new PersistentStreamConnection(
                STREAM_ID,
                mockConnectionManager,
                new AxonServerConfiguration(),
                new DelegatingEventConverter(new JacksonConverter()),
                properties,
                scheduler,
                UnitOfWorkTestUtils.SIMPLE_FACTORY,
                100
        );
    }

    @Test
    void consumesMessagesAndSendsAcknowledgements() {
        // given
        List<EventMessage> eventMessages = new LinkedList<>();
        testSubject.open((events, ctx) -> {
            eventMessages.addAll(events);
            return CompletableFuture.completedFuture(null);
        });
        MockPersistentStream mockPersistentStream = mockPersistentStreams.get(STREAM_ID);

        // when
        mockPersistentStream.publish(0, eventWithToken(0, "AggregateId-1", 0, "TestAggregate"));
        mockPersistentStream.publish(0, eventWithToken(1, "AggregateId-1", 1, "TestAggregate"));

        // then
        await().atMost(Duration.ofSeconds(1))
               .pollDelay(Duration.ofMillis(100))
               .until(() -> eventMessages.size() == 2);
        await().atMost(Duration.ofSeconds(1))
               .pollDelay(Duration.ofMillis(100))
               .until(() -> mockPersistentStream.lastAcknowledged(0) == 1);

        mockPersistentStream.closeSegment(0);
    }

    @Test
    void retryFailedHandler() {
        // given
        List<EventMessage> eventMessages = new LinkedList<>();
        AtomicInteger failureCountDown = new AtomicInteger(2);
        AtomicInteger attempts = new AtomicInteger();
        testSubject.open((events, ctx) -> {
            attempts.incrementAndGet();
            if (failureCountDown.getAndDecrement() > 0) {
                throw new IllegalStateException("Cannot invoke handler");
            }
            eventMessages.addAll(events);
            return CompletableFuture.completedFuture(null);
        });
        MockPersistentStream mockPersistentStream = mockPersistentStreams.get(STREAM_ID);

        // when
        mockPersistentStream.publish(0, eventWithToken(0, "AggregateId-1", 0, "TestAggregate"));
        mockPersistentStream.publish(0, eventWithToken(1, "AggregateId-1", 1, "TestAggregate"));

        // then — two failures + one success = at least 3 attempts; exact value 3 is not observable because
        // event0 and event1 are processed sequentially in the same processBatch call with no thread yield
        await().atMost(Duration.ofSeconds(5))
               .until(() -> attempts.get() >= 3);
        await().atMost(Duration.ofSeconds(1))
               .until(() -> eventMessages.size() == 2);
        await().atMost(Duration.ofSeconds(1))
               .until(() -> mockPersistentStream.lastAcknowledged(0) == 1);

        mockPersistentStream.closeSegment(0);
    }

    @Test
    void retryConsumerFailedFuture() {
        // given — fail twice then succeed
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger failuresLeft = new AtomicInteger(2);
        List<EventMessage> received = new LinkedList<>();

        testSubject.open((events, ctx) -> {
            attempts.incrementAndGet();
            if (failuresLeft.getAndDecrement() > 0) {
                return CompletableFuture.failedFuture(new IllegalStateException("transient"));
            }
            received.addAll(events);
            return CompletableFuture.completedFuture(null);
        });
        MockPersistentStream mockPersistentStream = mockPersistentStreams.get(STREAM_ID);

        // when
        mockPersistentStream.publish(0, eventWithToken(3, "agg-1", 0, "TestAggregate"));

        // then — retry engages (3 attempts), no ack until success
        await().atMost(Duration.ofSeconds(6))
               .until(() -> attempts.get() >= 3);
        await().atMost(Duration.ofSeconds(1))
               .until(() -> received.size() == 1);
        await().atMost(Duration.ofSeconds(1))
               .until(() -> mockPersistentStream.lastAcknowledged(0) == 3);

        mockPersistentStream.closeSegment(0);
    }

    @Test
    void givenAlreadyOpenedStreamWhenOpenOneMoreTimeThenException() {
        // given
        testSubject.open((events, ctx) -> CompletableFuture.completedFuture(null));

        // when / then
        assertThatThrownBy(() -> testSubject.open((events, ctx) -> CompletableFuture.completedFuture(null)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("stream-id: Persistent Stream has already been opened.");
    }

    @Test
    void givenAlreadyClosedStreamWhenOpenOneMoreTimeThenOpened() {
        // given
        testSubject.open((events, ctx) -> CompletableFuture.completedFuture(null));
        testSubject.close();

        // when / then — should not throw
        testSubject.open((events, ctx) -> CompletableFuture.completedFuture(null));
    }


    @Test
    void processBatch_propagatesOriginalExceptionType() {
        // given — consumer always returns a failed future
        testSubject.open((events, ctx) -> CompletableFuture.failedFuture(new IllegalStateException("fail on purpose")));
        MockPersistentStream mockPersistentStream = mockPersistentStreams.get(STREAM_ID);

        // when
        mockPersistentStream.publish(0, eventWithToken(0, "agg-1", 0, "TestAggregate"));

        // then — retry state is engaged (meaning the retry loop saw IllegalStateException, not CompletionException)
        // and no ack is sent while the consumer keeps failing
        await().atMost(Duration.ofSeconds(4))
               .pollDelay(Duration.ofMillis(100))
               .until(() -> mockPersistentStream.segments.containsKey(0));
        assertThat(mockPersistentStream.lastAcknowledged(0)).isEqualTo(-1);

        mockPersistentStream.closeSegment(0);
    }

    @Test
    void processBatch_acknowledgesOnlyAfterConsumerFutureCompletes() throws InterruptedException {
        // given
        CompletableFuture<Void> consumerGate = new CompletableFuture<>();
        testSubject.open((events, ctx) -> consumerGate);
        MockPersistentStream mockPersistentStream = mockPersistentStreams.get(STREAM_ID);

        // when — publish an event; the consumer blocks on consumerGate
        mockPersistentStream.publish(0, eventWithToken(5, "agg-1", 0, "TestAggregate"));

        // then — no ack before the gate opens
        Thread.sleep(300);
        assertThat(mockPersistentStream.lastAcknowledged(0)).isEqualTo(-1);

        // when — complete the future
        consumerGate.complete(null);

        // then — ack arrives after completion
        await().atMost(Duration.ofSeconds(2))
               .until(() -> mockPersistentStream.lastAcknowledged(0) == 5);

        mockPersistentStream.closeSegment(0);
    }



    @Test
    void processBatch_buildsEventMessageWithCorrectData() {
        // given
        List<EventMessage> received = new LinkedList<>();
        testSubject.open((events, ctx) -> {
            received.addAll(events);
            return CompletableFuture.completedFuture(null);
        });
        MockPersistentStream mockPersistentStream = mockPersistentStreams.get(STREAM_ID);

        String knownId = UUID.randomUUID().toString();
        long knownTimestamp = 1_700_000_000_000L;
        byte[] knownPayloadBytes = "hello".getBytes();

        String knownMetaKey = "testMeta";
        String knownMetaValue = "Metadata";
        EventWithToken evt = EventWithToken.newBuilder()
                                           .setToken(0)
                                           .setEvent(Event.newBuilder()
                                                          .setMessageIdentifier(knownId)
                                                          .setTimestamp(knownTimestamp)
                                                          .setPayload(SerializedObject.newBuilder()
                                                                                      .setType("MyEvent")
                                                                                      .setRevision("1")
                                                                                      .setData(ByteString.copyFrom(
                                                                                              knownPayloadBytes))
                                                                                      .build())
                                                          .putAllMetaData(Map.of(knownMetaKey,
                                                                                 MetaDataValue.newBuilder()
                                                                                              .setTextValue(
                                                                                                      knownMetaValue)
                                                                                              .build()))
                                                          .build())
                                           .build();

        // when
        mockPersistentStream.publish(0, evt);

        // then
        await().atMost(Duration.ofSeconds(2))
               .until(() -> received.size() == 1);

        EventMessage message = received.get(0);
        assertThat(message.identifier()).isEqualTo(knownId);
        assertThat(message.type().name()).isEqualTo("MyEvent");
        assertThat(message.type().version()).isEqualTo("1");
        assertThat((byte[]) message.payload()).isEqualTo(knownPayloadBytes);
        assertThat(message.metadata().get(knownMetaKey)).isEqualTo(knownMetaValue);
        assertThat(message.timestamp().toEpochMilli()).isEqualTo(knownTimestamp);

        mockPersistentStream.closeSegment(0);
    }

    @Test
    void processBatch_contextExposesTrackingToken() {
        // given — consumer records the tracking token and batch end token it receives for every invocation
        List<TrackingToken> capturedTrackingTokens = Collections.synchronizedList(new LinkedList<>());
        List<TrackingToken> capturedBatchEndtokens = Collections.synchronizedList(new LinkedList<>());
        testSubject.open((events, ctx) -> {
            capturedTrackingTokens.add(TrackingToken.fromContext(ctx).orElseThrow());
            capturedBatchEndtokens.add(ctx.getResource(TrackingToken.BATCH_END_RESOURCE_KEY));
            return CompletableFuture.completedFuture(null);
        });
        MockPersistentStream mockPersistentStream = mockPersistentStreams.get(STREAM_ID);

        // when — two events are enqueued before the availability notification fires, guaranteeing a single batch
        mockPersistentStream.publishBatch(0,
                                          eventWithToken(0, "agg-1", 0, "TestAggregate"),
                                          eventWithToken(1, "agg-1", 1, "TestAggregate"));

        // then — consumer is invoked once per event
        await().atMost(Duration.ofSeconds(2))
               .until(() -> capturedBatchEndtokens.size() == 2);
        assertThat(capturedBatchEndtokens.get(0))
                .describedAs("both events must share the same batch end token")
                .isSameAs(capturedBatchEndtokens.get(1));
        assertThat(capturedBatchEndtokens.get(0).position().orElseThrow())
                .describedAs("batch end token is 1")
                .isEqualTo(1);

        assertThat(capturedTrackingTokens.get(0).position().orElseThrow())
                .isEqualTo(0);
        assertThat(capturedTrackingTokens.get(1).position().orElseThrow())
                .isEqualTo(1);

        mockPersistentStream.closeSegment(0);
    }

    @Test
    void processBatch_contextExposesReplayToken() {
        // given — consumer records the tracking token and batch end token it receives for every invocation
        List<TrackingToken> capturedTrackingTokens = Collections.synchronizedList(new LinkedList<>());
        testSubject.open((events, ctx) -> {
            capturedTrackingTokens.add(TrackingToken.fromContext(ctx).orElseThrow());
            return CompletableFuture.completedFuture(null);
        });
        MockPersistentStream mockPersistentStream = mockPersistentStreams.get(STREAM_ID);

        // when — two events are enqueued before the availability notification fires, guaranteeing a single batch
        mockPersistentStream.publishBatch(0, true,
                                          eventWithToken(0, "agg-1", 0, "TestAggregate"),
                                          eventWithToken(1, "agg-1", 1, "TestAggregate"));

        // then — consumer is invoked once per event
        await().atMost(Duration.ofSeconds(2))
               .until(() -> capturedTrackingTokens.size() == 2);

        assertThat(capturedTrackingTokens.get(0))
                .isInstanceOf(ReplayToken.class);
        assertThat(capturedTrackingTokens.get(1))
                .isInstanceOf(ReplayToken.class);

        mockPersistentStream.closeSegment(0);
    }

    @Test
    void processBatch_contextExposesLegacyAggregateInformation() {
        // given — consumer records the tracking token and batch end token it receives for every invocation
        List<ProcessingContext> capturedContext = Collections.synchronizedList(new LinkedList<>());
        testSubject.open((events, ctx) -> {
            capturedContext.add(ctx);
            return CompletableFuture.completedFuture(null);
        });
        MockPersistentStream mockPersistentStream = mockPersistentStreams.get(STREAM_ID);

        // when — publishing an event
        mockPersistentStream.publish(0, eventWithToken(0, "agg-1", 0, "TestAggregate"));

        // then — consumer is invoked once per event
        await().atMost(Duration.ofSeconds(2))
               .until(() -> capturedContext.size() == 1);
        assertThat(capturedContext.getFirst().getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY))
                .describedAs("context supplies aggregate identifier")
                .isEqualTo("agg-1");
        assertThat(capturedContext.getFirst().getResource(LegacyResources.AGGREGATE_TYPE_KEY))
                .describedAs("context supplies aggregate type")
                .isEqualTo("TestAggregate");
        assertThat(capturedContext.getFirst().getResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY))
                .describedAs("context supplies aggregate identifier")
                .isEqualTo(0);

        mockPersistentStream.closeSegment(0);
    }

    @Test
    void allEventsInBatchShareSingleProcessingContext() {
        // given — consumer records the ProcessingContext it receives for every invocation
        List<ProcessingContext> capturedContexts = Collections.synchronizedList(new LinkedList<>());
        testSubject.open((events, ctx) -> {
            capturedContexts.add(ctx);
            return CompletableFuture.completedFuture(null);
        });
        MockPersistentStream mockPersistentStream = mockPersistentStreams.get(STREAM_ID);

        // when — two events are enqueued before the availability notification fires, guaranteeing a single batch
        mockPersistentStream.publishBatch(0,
                                          eventWithToken(0, "agg-1", 0, "TestAggregate"),
                                          eventWithToken(1, "agg-1", 1, "TestAggregate"));

        // then — consumer is invoked once per event, but both invocations share the same ProcessingContext
        await().atMost(Duration.ofSeconds(2))
               .until(() -> capturedContexts.size() == 2);
        assertThat(capturedContexts.get(0))
                .describedAs("both events in the same batch must share one ProcessingContext (one unit of work)")
                .isSameAs(capturedContexts.get(1));

        mockPersistentStream.closeSegment(0);
    }

    @Test
    void consecutiveBatchesGetSeparateProcessingContexts() {
        // given — consumer records each context; each event is published individually to force separate batches
        List<ProcessingContext> capturedContexts = Collections.synchronizedList(new LinkedList<>());
        testSubject.open((events, ctx) -> {
            capturedContexts.add(ctx);
            return CompletableFuture.completedFuture(null);
        });
        MockPersistentStream mockPersistentStream = mockPersistentStreams.get(STREAM_ID);

        // when — first event processed and acknowledged as its own batch
        mockPersistentStream.publish(0, eventWithToken(0, "agg-1", 0, "TestAggregate"));
        await().atMost(Duration.ofSeconds(2))
               .until(() -> mockPersistentStream.lastAcknowledged(0) == 0);

        // when — second event published only after the first batch is fully committed
        mockPersistentStream.publish(0, eventWithToken(1, "agg-1", 1, "TestAggregate"));
        await().atMost(Duration.ofSeconds(2))
               .until(() -> capturedContexts.size() == 2);

        // then — each batch created a fresh unit of work, so the contexts are distinct objects
        assertThat(capturedContexts.get(0))
                .describedAs("each batch must run in its own unit of work with a distinct ProcessingContext")
                .isNotSameAs(capturedContexts.get(1));

        mockPersistentStream.closeSegment(0);
    }

    @Test
    void unitOfWorkLifecycleCompletesBeforeAcknowledgement() {
        // given — the consumer registers an after-commit hook that records the ack state at that moment
        AtomicLong acknowledgedTokenAtAfterCommit = new AtomicLong(-2); // -2 signals "hook not yet fired"
        testSubject.open((events, ctx) -> {
            ctx.runOnAfterCommit(c ->
                                         // the ack must not have been sent yet — acknowledge() is called after joinAndUnwrap(executeWithResult(...)) returns
                                         acknowledgedTokenAtAfterCommit.compareAndSet(-2,
                                                                                      mockPersistentStreams.get(
                                                                                                                   STREAM_ID)
                                                                                                           .lastAcknowledged(
                                                                                                                   0))
            );
            return CompletableFuture.completedFuture(null);
        });
        MockPersistentStream mockPersistentStream = mockPersistentStreams.get(STREAM_ID);

        // when
        mockPersistentStream.publishBatch(0,
                                          eventWithToken(0, "agg-1", 0, "TestAggregate"),
                                          eventWithToken(1, "agg-1", 1, "TestAggregate"));

        // then — after-commit phase completes before acknowledge() is called, so no ack was present at hook time
        await().atMost(Duration.ofSeconds(2))
               .until(() -> acknowledgedTokenAtAfterCommit.get() != -2);
        assertThat(acknowledgedTokenAtAfterCommit.get())
                .describedAs("acknowledge() must not have been called yet when the after-commit hook fires")
                .isEqualTo(-1);

        // and the ack arrives after the lifecycle completes
        await().atMost(Duration.ofSeconds(2))
               .until(() -> mockPersistentStream.lastAcknowledged(0) == 1);

        mockPersistentStream.closeSegment(0);
    }

    @Test
    void streamClosedException_isHandledSilentlyWithoutReportingError() {
        // given — consumer records received events
        List<EventMessage> received = new LinkedList<>();
        testSubject.open((events, ctx) -> {
            received.addAll(events);
            return CompletableFuture.completedFuture(null);
        });
        MockPersistentStream mockStream = mockPersistentStreams.get(STREAM_ID);

        // when — nextIfAvailable() throws StreamClosedException; the event stays in the queue
        mockStream.publishFailing(0,
                                  new StreamClosedException(new RuntimeException("closed")),
                                  eventWithToken(0, "agg-1", 0, "TestAggregate"));

        // then — StreamClosed path: error() is never called; finally reschedules because peek() != null,
        //         and the pending event is consumed on the next run
        await().atMost(Duration.ofSeconds(2))
               .until(() -> received.size() == 1);
        assertThat(mockStream.errorWasReported(0))
                .describedAs("StreamClosedException must not trigger error() on the segment")
                .isFalse();

        mockStream.closeSegment(0);
    }

    @Test
    void unexpectedException_reportsErrorToSegment() {
        // given
        testSubject.open((events, ctx) -> CompletableFuture.completedFuture(null));
        MockPersistentStream mockStream = mockPersistentStreams.get(STREAM_ID);

        // when — nextIfAvailable() throws an unexpected RuntimeException (no events queued, so no reschedule)
        mockStream.publishFailing(0, new IllegalArgumentException("segment-read-failed"));

        // then — the exception catch block calls error() with the exception message
        await().atMost(Duration.ofSeconds(2))
               .until(() -> mockStream.errorWasReported(0));
        assertThat(mockStream.lastReportedError(0)).isEqualTo("segment-read-failed");

        // when


        mockStream.closeSegment(0);
    }

    @Test
    void interruptedException_reportsErrorToSegmentAndReinterruptsThread() {
        // given — consumer records received events
        List<EventMessage> received = new LinkedList<>();
        testSubject.open((events, ctx) -> {
            received.addAll(events);
            return CompletableFuture.completedFuture(null);
        });
        MockPersistentStream mockStream = mockPersistentStreams.get(STREAM_ID);

        // when — one event is readable (first nextIfAvailable succeeds), then nextIfAvailable(timeout)
        //         throws InterruptedException; this propagates through readBatch to the outer catch
        mockStream.publishWithInterruptOnTimeoutNext(0, eventWithToken(0, "agg-1", 0, "TestAggregate"));

        // then — the InterruptedException branch calls error() on the segment
        await().atMost(Duration.ofSeconds(2))
               .until(() -> mockStream.errorWasReported(0));
        assertThat(mockStream.lastReportedError(0)).isEqualTo("interrupted");
        // event was NOT acknowledged (exception aborted processBatch)
        assertThat(mockStream.lastAcknowledged(0)).isEqualTo(-1);

        mockStream.closeSegment(0);
    }

    @Test
    void pendingWorkDoneIsAcknowledgedWhenSegmentClosesAfterLastBatch() {
        // given — consumer processes events and closes the segment, simulating a server-initiated segment close
        List<EventMessage> received = new LinkedList<>();
        testSubject.open((events, ctx) -> {
            received.addAll(events);
            MockPersistentStreamSegment segment = mockPersistentStreams.get(STREAM_ID).segments.get(0);
            if (segment != null) {
                segment.close();
            }
            return CompletableFuture.completedFuture(null);
        });
        MockPersistentStream mockPersistentStream = mockPersistentStreams.get(STREAM_ID);

        // when
        mockPersistentStream.publish(0, eventWithToken(0, "agg-1", 0, "TestAggregate"));

        // then — event is consumed and the regular token is acknowledged
        await().atMost(Duration.ofSeconds(2))
               .until(() -> received.size() == 1);
        // then — PENDING_WORK_DONE_MARKER is acknowledged because the segment was closed after the final batch
        await().atMost(Duration.ofSeconds(2))
               .until(() -> mockPersistentStream.lastAcknowledged(0) == PersistentStreamSegment.PENDING_WORK_DONE_MARKER);

        mockPersistentStream.closeSegment(0);
    }

    private static EventWithToken eventWithToken(int token, String aggregateId, int seqNr, String aggregateType) {
        return EventWithToken.newBuilder()
                             .setToken(token)
                             .setEvent(Event.newBuilder()
                                            .setAggregateIdentifier(aggregateId)
                                            .setAggregateSequenceNumber(seqNr)
                                            .setAggregateType(aggregateType)
                                            .setMessageIdentifier(UUID.randomUUID().toString())
                                            .setPayload(SerializedObject.newBuilder()
                                                                        .setType("string")
                                                                        .setRevision("1"))
                                            .setTimestamp(System.currentTimeMillis()))
                             .build();
    }

    private static class MockPersistentStream implements PersistentStream {

        private final PersistentStreamCallbacks callbacks;
        final Map<Integer, MockPersistentStreamSegment> segments = new ConcurrentHashMap<>();

        public MockPersistentStream(PersistentStreamCallbacks callbacks) {
            this.callbacks = callbacks;
        }

        @Override
        public void close() {
            callbacks.onClosed();
        }

        private void publish(int segmentNumber, EventWithToken eventWithToken) {
            publish(segmentNumber, false, eventWithToken);
        }

        private void publish(int segmentNumber, boolean isReplay, EventWithToken eventWithToken) {
            @SuppressWarnings("resource")
            MockPersistentStreamSegment segment = segments.computeIfAbsent(segmentNumber, i -> {
                MockPersistentStreamSegment mockSegment = new MockPersistentStreamSegment(i);
                callbacks.onSegmentOpened().accept(mockSegment);
                mockSegment.onAvailable(() -> callbacks.onAvailable().accept(mockSegment));
                return mockSegment;
            });
            segment.entries.add(PersistentStreamEvent.newBuilder().setReplay(isReplay).setEvent(eventWithToken)
                                                     .build());
            segment.onAvailable.run();
        }

        /**
         * Enqueues all {@code events} into the segment's queue <em>before</em> firing the single availability
         * notification. This guarantees that all events are visible to {@code readBatch} in a single pass, so they end
         * up in the same batch and are processed within the same unit of work.
         */
        private void publishBatch(int segmentNumber, EventWithToken... events) {
            publishBatch(segmentNumber, false, events);
        }

        /**
         * Enqueues all {@code events} into the segment's queue <em>before</em> firing the single availability
         * notification. This guarantees that all events are visible to {@code readBatch} in a single pass, so they end
         * up in the same batch and are processed within the same unit of work.
         */
        private void publishBatch(int segmentNumber, boolean isReplay, EventWithToken... events) {
            @SuppressWarnings("resource")
            MockPersistentStreamSegment segment = segments.computeIfAbsent(segmentNumber, i -> {
                MockPersistentStreamSegment mockSegment = new MockPersistentStreamSegment(i);
                callbacks.onSegmentOpened().accept(mockSegment);
                mockSegment.onAvailable(() -> callbacks.onAvailable().accept(mockSegment));
                return mockSegment;
            });
            for (EventWithToken evt : events) {
                segment.entries.add(PersistentStreamEvent.newBuilder().setReplay(isReplay).setEvent(evt).build());
            }
            // single notification after all events are enqueued — they will all be read in one readBatch call
            segment.onAvailable.run();
        }

        /**
         * Creates the segment (if absent) and fires one availability notification after setting
         * {@code failWith} as the exception to throw on the next {@code nextIfAvailable()} call.
         * Any supplied {@code events} are added to the segment's queue <em>before</em> the exception is
         * armed so that {@code peek()} still returns a non-null value, causing the {@code finally} block
         * to reschedule processing.
         */
        private void publishFailing(int segmentNumber, RuntimeException failWith, EventWithToken... events) {
            @SuppressWarnings("resource")
            MockPersistentStreamSegment segment = segments.computeIfAbsent(segmentNumber, i -> {
                MockPersistentStreamSegment mockSegment = new MockPersistentStreamSegment(i);
                callbacks.onSegmentOpened().accept(mockSegment);
                mockSegment.onAvailable(() -> callbacks.onAvailable().accept(mockSegment));
                return mockSegment;
            });
            for (EventWithToken evt : events) {
                segment.entries.add(PersistentStreamEvent.newBuilder().setEvent(evt).build());
            }
            segment.failNextWith(failWith);
            segment.onAvailable.run();
        }

        /**
         * Creates the segment (if absent), enqueues {@code firstEvent}, arms an
         * {@link InterruptedException} on the timeout-variant of {@code nextIfAvailable}, and fires
         * the availability notification. The first {@code nextIfAvailable()} succeeds (adding the event
         * to the batch), and the subsequent {@code nextIfAvailable(long, TimeUnit)} throws.
         */
        private void publishWithInterruptOnTimeoutNext(int segmentNumber, EventWithToken firstEvent) {
            @SuppressWarnings("resource")
            MockPersistentStreamSegment segment = segments.computeIfAbsent(segmentNumber, i -> {
                MockPersistentStreamSegment mockSegment = new MockPersistentStreamSegment(i);
                callbacks.onSegmentOpened().accept(mockSegment);
                mockSegment.onAvailable(() -> callbacks.onAvailable().accept(mockSegment));
                return mockSegment;
            });
            segment.entries.add(PersistentStreamEvent.newBuilder().setEvent(firstEvent).build());
            segment.throwInterruptedOnTimeoutNext.set(true);
            segment.onAvailable.run();
        }

        public void closeSegment(int segmentNumber) {
            MockPersistentStreamSegment segment = segments.remove(segmentNumber);
            if (segment != null) {
                callbacks.onSegmentClosed().accept(segment);
            }
        }

        public long lastAcknowledged(int segmentNumber) {
            MockPersistentStreamSegment segment = segments.get(segmentNumber);
            return segment == null ? -1 : segment.lastAcknowledged.get();
        }

        public boolean errorWasReported(int segmentNumber) {
            MockPersistentStreamSegment segment = segments.get(segmentNumber);
            return segment != null && segment.errorWasReported.get();
        }

        @Nullable
        public String lastReportedError(int segmentNumber) {
            MockPersistentStreamSegment segment = segments.get(segmentNumber);
            return segment == null ? null : segment.lastReportedError.get();
        }
    }

    private static class MockPersistentStreamSegment implements PersistentStreamSegment {

        private final ConcurrentLinkedDeque<PersistentStreamEvent> entries = new ConcurrentLinkedDeque<>();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final int segment;
        private Runnable onAvailable = () -> {
        };
        final AtomicLong lastAcknowledged = new AtomicLong(-1);
        // exception to throw on the next nextIfAvailable() call (cleared after use)
        private final AtomicReference<RuntimeException> scheduledFailure = new AtomicReference<>();
        // when true, nextIfAvailable(long, TimeUnit) throws InterruptedException once
        final AtomicBoolean throwInterruptedOnTimeoutNext = new AtomicBoolean(false);
        final AtomicBoolean errorWasReported = new AtomicBoolean(false);
        final AtomicReference<@Nullable String> lastReportedError = new AtomicReference<>();

        public void failNextWith(RuntimeException ex) {
            scheduledFailure.set(ex);
        }

        private MockPersistentStreamSegment(int segment) {
            this.segment = segment;
        }

        @Override
        public PersistentStreamEvent peek() {
            return entries.peek();
        }

        @Override
        public PersistentStreamEvent nextIfAvailable() {
            RuntimeException ex = scheduledFailure.getAndSet(null);
            if (ex != null) {
                throw ex;
            }
            return entries.isEmpty() ? null : entries.removeFirst();
        }

        @Override
        public PersistentStreamEvent nextIfAvailable(long timeout, TimeUnit unit) throws InterruptedException {
            if (throwInterruptedOnTimeoutNext.getAndSet(false)) {
                throw new InterruptedException("interrupted");
            }
            long endTime = System.currentTimeMillis() + unit.toMillis(timeout);
            PersistentStreamEvent event = nextIfAvailable();
            while (event == null && System.currentTimeMillis() < endTime && !closed.get()) {
                Thread.sleep(1);
                event = nextIfAvailable();
            }
            return event;
        }

        @Override
        public PersistentStreamEvent next() throws InterruptedException {
            PersistentStreamEvent event = nextIfAvailable();
            while (event == null && !closed.get()) {
                Thread.sleep(1);
                event = nextIfAvailable();
            }
            return event;
        }

        @Override
        public void onAvailable(Runnable callback) {
            this.onAvailable = callback;
        }

        @Override
        public void close() {
            closed.set(true);
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public Optional<Throwable> getError() {
            return Optional.empty();
        }

        @Override
        public void onSegmentClosed(Runnable callback) {
            // not required for testing
        }

        @Override
        public void acknowledge(long token) {
            lastAcknowledged.set(token);
        }

        @Override
        public void error(@Nullable String error) {
            errorWasReported.set(true);
            lastReportedError.set(error);
        }

        @Override
        public int segment() {
            return segment;
        }

        private void publish(EventWithToken eventWithToken) {
            entries.add(PersistentStreamEvent.newBuilder().setEvent(eventWithToken).build());
            onAvailable.run();
        }
    }
}
