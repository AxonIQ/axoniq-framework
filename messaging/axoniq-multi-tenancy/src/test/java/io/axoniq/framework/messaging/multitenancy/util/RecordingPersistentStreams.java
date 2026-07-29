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

package io.axoniq.framework.messaging.multitenancy.util;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.PersistentStream;
import io.axoniq.axonserver.connector.event.PersistentStreamCallbacks;
import io.axoniq.axonserver.connector.event.PersistentStreamSegment;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.streams.PersistentStreamEvent;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test double serving an in-memory {@link PersistentStream} per Axon Server context, so a multi-tenant persistent stream
 * source can be exercised without a running Axon Server.
 * <p>
 * Records which contexts had a stream opened, lets a test publish events into a specific context's stream, and reports
 * which streams were closed again, which is what makes per-tenant isolation observable.
 */
public class RecordingPersistentStreams {

    private final AxonServerConnectionManager connectionManager = mock(AxonServerConnectionManager.class);
    private final Map<String, RecordingPersistentStream> streamsByContext = new ConcurrentHashMap<>();

    /**
     * Creates a recording set of persistent streams, wiring a mocked {@link AxonServerConnectionManager} that opens an
     * in-memory stream per requested context.
     */
    public RecordingPersistentStreams() {
        when(connectionManager.getConnection(anyString())).thenAnswer(connectionInvocation -> {
            String context = connectionInvocation.getArgument(0);
            AxonServerConnection connection = mock(AxonServerConnection.class);
            EventChannel eventChannel = mock(EventChannel.class);
            when(eventChannel.openPersistentStream(anyString(), anyInt(), anyInt(), any(), any()))
                    .thenAnswer(streamInvocation -> streamsByContext.compute(
                            context,
                            (ignored, previous) -> new RecordingPersistentStream(streamInvocation.getArgument(3))
                    ));
            when(connection.eventChannel()).thenReturn(eventChannel);
            return connection;
        });
    }

    /**
     * Returns the mocked connection manager to hand to the component under test.
     *
     * @return the mocked {@link AxonServerConnectionManager}
     */
    public AxonServerConnectionManager connectionManager() {
        return connectionManager;
    }

    /**
     * Returns the contexts a persistent stream was opened for, whether it is still open or already closed.
     *
     * @return the contexts a stream was opened for
     */
    public List<String> openedContexts() {
        return List.copyOf(streamsByContext.keySet());
    }

    /**
     * Indicates whether the stream of the given {@code context} is currently open.
     *
     * @param context the Axon Server context to check
     * @return {@code true} when a stream was opened for the given {@code context} and not closed since
     */
    public boolean hasOpenStream(String context) {
        RecordingPersistentStream stream = streamsByContext.get(context);
        return stream != null && !stream.closed.get();
    }

    /**
     * Publishes an event of the given {@code eventName} into the stream of the given {@code context}, on segment zero.
     * <p>
     * The name doubles as the event's correlation handle: it arrives on the consumed
     * {@link org.axonframework.messaging.eventhandling.EventMessage} as its {@code type().name()}, so a test can tell
     * which published event it is looking at without converting a payload.
     *
     * @param context   the Axon Server context whose stream receives the event
     * @param token     the global token of the published event
     * @param eventName the name identifying the published event
     * @throws IllegalStateException if no stream was opened for the given {@code context}
     */
    public void publish(String context, long token, String eventName) {
        RecordingPersistentStream stream = streamsByContext.get(context);
        if (stream == null) {
            throw new IllegalStateException("No persistent stream was opened for context [" + context + "].");
        }
        stream.publish(token, eventName);
    }

    private static final class RecordingPersistentStream implements PersistentStream {

        private final PersistentStreamCallbacks callbacks;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final Map<Integer, RecordingPersistentStreamSegment> segments = new ConcurrentHashMap<>();

        private RecordingPersistentStream(PersistentStreamCallbacks callbacks) {
            this.callbacks = callbacks;
        }

        @Override
        public void close() {
            closed.set(true);
            segments.values().forEach(RecordingPersistentStreamSegment::close);
            callbacks.onClosed().accept(null);
        }

        private void publish(long token, String eventName) {
            @SuppressWarnings("resource")
            RecordingPersistentStreamSegment segment = segments.computeIfAbsent(0, segmentNumber -> {
                RecordingPersistentStreamSegment created = new RecordingPersistentStreamSegment(segmentNumber);
                callbacks.onSegmentOpened().accept(created);
                created.onAvailable(() -> callbacks.onAvailable().accept(created));
                return created;
            });
            segment.publish(token, eventName);
        }
    }

    private static final class RecordingPersistentStreamSegment implements PersistentStreamSegment {

        private final ConcurrentLinkedDeque<PersistentStreamEvent> entries = new ConcurrentLinkedDeque<>();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicLong lastAcknowledged = new AtomicLong(-1);
        private final int segment;

        private Runnable onAvailable = () -> {
        };

        private RecordingPersistentStreamSegment(int segment) {
            this.segment = segment;
        }

        private void publish(long token, String eventName) {
            entries.add(PersistentStreamEvent.newBuilder()
                                             .setEvent(eventWithToken(token, eventName))
                                             .build());
            onAvailable.run();
        }

        @Override
        public @Nullable PersistentStreamEvent peek() {
            return entries.peek();
        }

        @Override
        public @Nullable PersistentStreamEvent nextIfAvailable() {
            return entries.isEmpty() ? null : entries.removeFirst();
        }

        @Override
        public @Nullable PersistentStreamEvent nextIfAvailable(long timeout, TimeUnit unit)
                throws InterruptedException {
            long endTime = System.currentTimeMillis() + unit.toMillis(timeout);
            PersistentStreamEvent event = nextIfAvailable();
            while (event == null && System.currentTimeMillis() < endTime && !closed.get()) {
                Thread.sleep(1);
                event = nextIfAvailable();
            }
            return event;
        }

        @Override
        public @Nullable PersistentStreamEvent next() throws InterruptedException {
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
            // not required for testing
        }

        @Override
        public int segment() {
            return segment;
        }

        // The event name is carried as the payload type, so a consumer can correlate the event it received with the
        // one a test published straight off its MessageType, with no payload conversion involved.
        private static EventWithToken eventWithToken(long token, String eventName) {
            return EventWithToken.newBuilder()
                                 .setToken(token)
                                 .setEvent(Event.newBuilder()
                                                .setMessageIdentifier(UUID.randomUUID().toString())
                                                .setTimestamp(System.currentTimeMillis())
                                                .setPayload(SerializedObject.newBuilder()
                                                                            .setType(eventName)
                                                                            .setRevision("0")
                                                                            .setData(ByteString.copyFrom(
                                                                                    "{}".getBytes()))
                                                                            .build())
                                                .build())
                                 .build();
        }
    }
}
