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

import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.PersistentStream;
import io.axoniq.axonserver.connector.event.PersistentStreamCallbacks;
import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.axonserver.connector.event.PersistentStreamSegment;
import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.streams.PersistentStreamEvent;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.shared.MetadataConverter;
import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static io.axoniq.axonserver.connector.event.PersistentStreamSegment.PENDING_WORK_DONE_MARKER;

/**
 * A connection instance receiving the events for a persistent stream to pass on in batches to an event consumer.
 * <p>
 * Opens a gRPC connection to Axon Server via {@link #open(BiFunction)} and invokes the supplied {@link BiFunction} for
 * each batch. The batch consumer returns a {@link CompletableFuture} that must complete before the token for the last
 * event in the batch is acknowledged to Axon Server. On consumer failure the batch is retried with exponential
 * back-off.
 * <p>
 * {@link #close()} still delivers every event already buffered locally at the moment it is called to the consumer, and
 * only retires that consumer, in favor of a no-op, once all of it has been delivered.
 * <p>
 * This is an internal helper for the {@link PersistentStreamEventSource} managing the gRPC based persistent stream with
 * Axon Server. This class is usually not used directly by users.
 *
 * @author Marc Gathier
 * @author Jakob Hatzl
 * @since 5.2.0
 */
@Internal
public class PersistentStreamConnection {

    private static final int MAX_RETRY_INTERVAL_SECONDS = 60;
    private static final int MIN_RETRY_INTERVAL_SECONDS = 1;
    private static final BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>>
            NO_OP_CONSUMER = (events, ctx) -> CompletableFuture.completedFuture(null);
    private static final Logger logger = LoggerFactory.getLogger(PersistentStreamConnection.class);

    private final String streamId;
    private final AxonServerConnectionManager connectionManager;
    private final AxonServerConfiguration serverConfig;
    private final EventConverter converter;
    private final EventTypeResolver eventTypeResolver;
    private final PersistentStreamProperties persistentStreamProperties;

    private final AtomicReference<@Nullable PersistentStream> persistentStreamHolder = new AtomicReference<>();
    private final AtomicBoolean opened = new AtomicBoolean(false);
    // tags every open() so a reconnect scheduled for an earlier one can be told apart from the current one
    private final AtomicInteger openGeneration = new AtomicInteger();

    private final AtomicReference<BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>>>
            consumer = new AtomicReference<>(NO_OP_CONSUMER);

    private final ScheduledExecutorService scheduler;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final PersistentStreamContextCustomizer contextCustomizer;
    private final int batchSize;
    private final Map<Integer, SegmentConnection> segments = new ConcurrentHashMap<>();
    private final AtomicInteger retrySeconds = new AtomicInteger(MIN_RETRY_INTERVAL_SECONDS);

    private final @Nullable String context;

    /**
     * Instantiates a {@code PersistentStreamConnection} falling back to the
     * {@link EventTypeResolver#DEFAULT default event type resolver} for message type resolution.
     *
     * @param streamId                   the unique identifier of the persistent stream
     * @param connectionManager          the Axon Server connection manager
     * @param serverConfig               the Axon Server configuration
     * @param converter                  the event converter used to deserialize event payloads
     * @param persistentStreamProperties the properties for the persistent stream
     * @param scheduler                  the scheduler thread pool to schedule tasks
     * @param unitOfWorkFactory          the unit of work factory
     * @param batchSize                  the maximum number of events to collect per batch
     * @param context                    the Axon Server context to connect to, or {@code null} to use
     *                                   {@link AxonServerConfiguration#getContext()}
     */
    public PersistentStreamConnection(String streamId,
                                      AxonServerConnectionManager connectionManager,
                                      AxonServerConfiguration serverConfig,
                                      EventConverter converter,
                                      PersistentStreamProperties persistentStreamProperties,
                                      ScheduledExecutorService scheduler,
                                      UnitOfWorkFactory unitOfWorkFactory,
                                      int batchSize,
                                      @Nullable String context) {
        this(streamId,
             connectionManager,
             serverConfig,
             converter,
             EventTypeResolver.DEFAULT,
             persistentStreamProperties,
             scheduler,
             unitOfWorkFactory,
             PersistentStreamContextCustomizer.NO_OP,
             batchSize,
             context);
    }

    /**
     * Instantiates a {@code PersistentStreamConnection} placing additional resources on the {@link ProcessingContext}
     * of every batch through the given {@code contextCustomizer}.
     *
     * @param streamId                   the unique identifier of the persistent stream
     * @param connectionManager          the Axon Server connection manager
     * @param serverConfig               the Axon Server configuration
     * @param converter                  the event converter used to deserialize event payloads
     * @param eventTypeResolver          the event type resolver used to resolve the type on inbound events
     * @param persistentStreamProperties the properties for the persistent stream
     * @param scheduler                  the scheduler thread pool to schedule tasks
     * @param unitOfWorkFactory          the unit of work factory
     * @param contextCustomizer          the customizer placing resources on the {@link ProcessingContext} of every
     *                                   batch, invoked once per batch before any of its events is consumed, returning
     *                                   the context that batch is consumed with
     * @param batchSize                  the maximum number of events to collect per batch
     * @param context                    the Axon Server context to connect to, or {@code null} to use
     *                                   {@link AxonServerConfiguration#getContext()}
     */
    public PersistentStreamConnection(String streamId,
                                      AxonServerConnectionManager connectionManager,
                                      AxonServerConfiguration serverConfig,
                                      EventConverter converter,
                                      EventTypeResolver eventTypeResolver,
                                      PersistentStreamProperties persistentStreamProperties,
                                      ScheduledExecutorService scheduler,
                                      UnitOfWorkFactory unitOfWorkFactory,
                                      PersistentStreamContextCustomizer contextCustomizer,
                                      int batchSize,
                                      @Nullable String context) {
        this.streamId = Objects.requireNonNull(streamId, "streamId must not be null");
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
        this.serverConfig = Objects.requireNonNull(serverConfig, "serverConfig must not be null");
        this.converter = Objects.requireNonNull(converter, "converter must not be null");
        this.eventTypeResolver = Objects.requireNonNull(eventTypeResolver, "eventTypeResolver must not be null");
        this.persistentStreamProperties = Objects.requireNonNull(persistentStreamProperties,
                                                                 "persistentStreamProperties must not be null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "unitOfWorkFactory must not be null");
        this.contextCustomizer = Objects.requireNonNull(contextCustomizer, "contextCustomizer must not be null");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive, but was: " + batchSize);
        }
        this.batchSize = batchSize;
        this.context = context;
    }

    /**
     * Initiates the connection to Axon Server and starts delivering events to the given {@code consumer}.
     * <p>
     * The stream can be opened with only a single consumer at a time. After a previous {@link #close()}, the stream may
     * be reopened by calling this method again.
     *
     * @param consumer the consumer of batches of event messages; to allow providing tracking and replay information per
     *                 event, it receives each event in a single callback {@link ProcessingContext} enriched with the
     *                 current {@link TrackingToken} and, when available, aggregate identity information, and must
     *                 return a {@link CompletableFuture} that completes when the event has been processed; events are
     *                 still processed in batches as configured through the {@code batchSize} constructor argument, the
     *                 persistent stream connection takes care of creating and spanning a unit of work over all events
     *                 belonging to a single batch
     * @throws IllegalStateException if the stream was already opened
     */
    public void open(BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> consumer) {
        if (!opened.compareAndSet(false, true)) {
            throw new IllegalStateException(String.format("%s: Persistent Stream has already been opened.", streamId));
        }
        openGeneration.incrementAndGet();
        this.consumer.set(consumer);
        start();
    }

    private void start() {
        String context = this.context != null && !this.context.isEmpty()
                ? this.context
                : serverConfig.getContext();
        PersistentStreamCallbacks callbacks = new PersistentStreamCallbacks(this::segmentOpened,
                                                                            this::segmentClosed,
                                                                            this::messageAvailable,
                                                                            this::streamClosed);
        // handles both non-dcb and dcb-contexts
        EventChannel eventChannel = connectionManager.getConnection(context).eventChannel();
        PersistentStream persistentStream = eventChannel.openPersistentStream(
                streamId,
                serverConfig.getEventFlowControl().getPermits(),
                serverConfig.getEventFlowControl().getNrOfNewPermits(),
                callbacks,
                persistentStreamProperties
        );
        persistentStreamHolder.set(persistentStream);
    }

    private void segmentOpened(PersistentStreamSegment persistentStreamSegment) {
        logger.info("Segment opened: {}", persistentStreamSegment);
        retrySeconds.set(MIN_RETRY_INTERVAL_SECONDS);
        segments.put(
                persistentStreamSegment.segment(), new SegmentConnection(persistentStreamSegment, consumer.get())
        );
    }

    private void segmentClosed(PersistentStreamSegment persistentStreamSegment) {
        segments.remove(persistentStreamSegment.segment());
        logger.info("Segment closed: {}", persistentStreamSegment);
    }

    private void messageAvailable(PersistentStreamSegment persistentStreamSegment) {
        SegmentConnection segmentConnection = segments.get(persistentStreamSegment.segment());
        if (segmentConnection != null) {
            segmentConnection.messageAvailable();
        }
    }

    private void streamClosed(@Nullable Throwable throwable) {
        persistentStreamHolder.set(null);
        if (throwable != null && opened.get()) {
            // Only reschedule reconnection if the stream was NOT intentionally closed.
            // close() flips opened to false, preventing reconnection attempts.
            logger.info("{}: Rescheduling persistent stream", streamId, throwable);
            int generation = openGeneration.get();
            scheduler.schedule(() -> reconnect(generation),
                               retrySeconds.getAndUpdate(current -> Math.min(MAX_RETRY_INTERVAL_SECONDS, current * 2)),
                               TimeUnit.SECONDS);
        }
    }

    private void reconnect(int generation) {
        // a reconnect scheduled before close(), or before a close() and reopen, belongs to a stream that is
        // already gone: starting it now would open a stream nobody reads from or closes, or a second one
        // next to the one the reopen already started
        if (opened.get() && openGeneration.get() == generation) {
            start();
        }
    }

    /**
     * Closes the persistent stream connection to Axon Server.
     * <p>
     * Events already buffered locally for a segment at the moment this method is called are still delivered to the
     * consumer supplied to {@link #open(BiFunction)}. The consumer is only retired, in favor of a no-op, once every
     * currently open segment has confirmed its local buffer is fully drained. Otherwise, those buffered events would be
     * acknowledged to Axon Server without ever reaching a real consumer.
     */
    public void close() {
        BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> consumerToRetire =
                consumer.get();
        List<SegmentConnection> activeSegments = List.copyOf(segments.values());
        activeSegments.forEach(SegmentConnection::requestClose);
        CompletableFuture.allOf(activeSegments.stream().map(SegmentConnection::drained)
                                              .toArray(CompletableFuture[]::new))
                         .thenRun(() -> {
                             // a CAS, not a plain set: if open() has since installed a new consumer, this
                             // retirement of the old one must not clobber it
                             consumer.compareAndSet(consumerToRetire, NO_OP_CONSUMER);
                         });
        PersistentStream persistentStream = persistentStreamHolder.getAndSet(null);
        // set last: a concurrent open() installing a new stream must not have it closed by this call
        opened.set(false);
        if (persistentStream != null) {
            persistentStream.close();
        }
    }

    private interface SegmentState {

        void readMessages();
    }

    private class SegmentConnection {

        private final AtomicBoolean processGate = new AtomicBoolean();
        private final CompletableFuture<Void> drained = new CompletableFuture<>();
        private final AtomicBoolean closeRequested = new AtomicBoolean();
        private final PersistentStreamSegment persistentStreamSegment;
        private final BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> segmentConsumer;
        private final AtomicReference<SegmentState> currentState = new AtomicReference<>(new ProcessingState());

        public SegmentConnection(
                PersistentStreamSegment persistentStreamSegment,
                BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> segmentConsumer
        ) {
            this.persistentStreamSegment = persistentStreamSegment;
            this.segmentConsumer = segmentConsumer;
        }

        void requestClose() {
            closeRequested.set(true);
        }

        /**
         * A future completing once this segment has no work left: either its buffer was consumed up to the terminal
         * marker and {@link PersistentStreamSegment#PENDING_WORK_DONE_MARKER} was acknowledged, or the batch it was
         * retrying was abandoned because {@link PersistentStreamConnection#close()} was called. Completes for every
         * segment close, whether requested through {@code close()} or initiated by Axon Server.
         *
         * @return a future completing once this segment has no work left
         */
        CompletableFuture<Void> drained() {
            return drained;
        }

        private class RetryState implements SegmentState {

            private final List<PersistentStreamEvent> batch;
            private final AtomicInteger retryInterval = new AtomicInteger(MIN_RETRY_INTERVAL_SECONDS);

            public RetryState(List<PersistentStreamEvent> batch) {
                this.batch = batch;
                scheduler.schedule(this::retry, retryInterval.get(), TimeUnit.SECONDS);
            }

            private void retry() {
                if (closeRequested.get()) {
                    // the acknowledgement can no longer reach Axon Server at this point anyway. The real connector's
                    // close() only waits briefly for a done marker before tearing down the outbound stream, so retrying
                    // further would just hold up this segment's drain for nothing
                    drained.complete(null);
                    return;
                }
                processBatch(batch)
                        .thenRun(() -> {
                            currentState.set(new ProcessingState());
                            scheduler.submit(SegmentConnection.this::readMessagesFromSegment);
                        }).exceptionally(ex -> {
                            int interval = retryInterval.updateAndGet(old -> Math.min(old * 2, MAX_RETRY_INTERVAL_SECONDS));
                            logger.warn("{}: Exception while retrying events for segment {}, retrying after {} seconds",
                                        streamId, persistentStreamSegment.segment(), interval, ex);
                            scheduler.schedule(this::retry, interval, TimeUnit.SECONDS);
                            return null;
                        });
            }

            @Override
            public void readMessages() {
                // no-op — first need to retry the cached events
            }
        }

        private class ProcessingState implements SegmentState {

            @Override
            public void readMessages() {
                if (!processGate.compareAndSet(false, true)) {
                    return;
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("{}[{}] readMessagesFromSegment - closed: {}",
                                 streamId, persistentStreamSegment.segment(), persistentStreamSegment.isClosed());
                }

                readBatch(persistentStreamSegment)
                        .handle((batch, ex) -> {
                            if (ex == null) {
                                return processBatch(batch)
                                        .exceptionally(fla -> {
                                            logger.warn(
                                                    "{}: Exception while processing events for segment {}, retrying after {} second",
                                                    streamId,
                                                    persistentStreamSegment.segment(),
                                                    MIN_RETRY_INTERVAL_SECONDS,
                                                    fla
                                            );
                                            currentState.set(new RetryState(batch));
                                            return null;
                                        });
                            } else {
                                switch (ex) {
                                    case StreamClosedException sce:
                                        logger.debug("{}: Stream closed for segment {}",
                                                     streamId,
                                                     persistentStreamSegment.segment());
                                        break;
                                    case InterruptedException ie:
                                        Thread.currentThread().interrupt();
                                    default:
                                        persistentStreamSegment.error(ex.getMessage());
                                        logger.warn("{}: Exception while processing events for segment {}",
                                                    streamId, persistentStreamSegment.segment(), ex);
                                        break;
                                }
                            }
                            return CompletableFuture.<Void>completedFuture(null);
                        }).thenCompose(f -> f)
                        .whenComplete((ignored, ex) -> {
                            if (ex == null) {
                                acknowledgeDoneWhenClosed(persistentStreamSegment);
                            }
                            processGate.set(false);
                            // covers a missed wakeup: a messageAvailable() call for the terminal marker arriving
                            // while processGate was still true is otherwise never retried, since peek() is null
                            // once that marker is the only thing left to consume
                            if (persistentStreamSegment.peek() != null
                                    || (persistentStreamSegment.isClosed() && !drained.isDone())) {
                                scheduler.submit(SegmentConnection.this::readMessagesFromSegment);
                            }
                        });
            }

            private CompletableFuture<List<PersistentStreamEvent>> readBatch(
                    PersistentStreamSegment persistentStreamSegment
            ) {
                if (!persistentStreamSegment.isClosed()) {
                    List<PersistentStreamEvent> batch = new LinkedList<>();
                    try {
                        PersistentStreamEvent event = persistentStreamSegment.nextIfAvailable();
                        if (event == null) {
                            return CompletableFuture.completedFuture(batch);
                        }
                        batch.add(event);
                        while (batch.size() < batchSize && !persistentStreamSegment.isClosed()
                                && (event = persistentStreamSegment.nextIfAvailable(1, TimeUnit.MILLISECONDS))
                                != null) {
                            batch.add(event);
                        }
                        return CompletableFuture.completedFuture(batch);
                    } catch (Exception e) {
                        return CompletableFuture.failedFuture(e);
                    }
                }
                return CompletableFuture.completedFuture(List.of());
            }

            private void acknowledgeDoneWhenClosed(PersistentStreamSegment persistentStreamSegment) {
                // only this ProcessingState may confirm the drain — if a batch failed in the meantime,
                // currentState has already moved to a RetryState, and that failed batch still needs to be
                // retried through the real consumer before the segment is actually drained
                if (persistentStreamSegment.isClosed() && currentState.get() == this && drained.complete(null)) {
                    persistentStreamSegment.acknowledge(PENDING_WORK_DONE_MARKER);
                }
            }
        }

        /**
         * Processes the given {@code batch} of events.
         * <p>
         * Once a {@code batch} is dequeued from the {@link PersistentStreamSegment segment's} buffer, it must
         * <b>always</b> be processed and acknowledged. Doing so ensures we process through events given by the
         * {@code PersistentStreamSegment} before it may complete (exceptionally). If we'd stop processing before that
         * because {@link PersistentStreamSegment#isClosed()} is {@code true}, we may thus skip events.
         *
         * @param batch the batch of events to process
         * @return a future completing when all events in the given {@code batch} have been processed
         */
        private CompletableFuture<Void> processBatch(List<PersistentStreamEvent> batch) {
            if (batch.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }

            PersistentStreamEvent batchLastEvent = batch.getLast();
            long token = batchLastEvent.getEvent().getToken();
            TrackingToken batchEndToken = createToken(batchLastEvent);
            UnitOfWork unitOfWork = unitOfWorkFactory.create();

            return unitOfWork.executeWithResult(processingContext -> {
                CompletableFuture<?> result = CompletableFuture.completedFuture(null);
                // Applied before the first event is consumed, so every event of this batch observes the
                // batch-constant resources, such as the tenant a per-tenant stream belongs to.
                ProcessingContext batchContext =
                        contextCustomizer.apply(processingContext)
                                         .withResource(TrackingToken.BATCH_END_RESOURCE_KEY, batchEndToken);
                for (PersistentStreamEvent pse : batch) {
                    result = result.thenCompose(
                            ignored -> segmentConsumer.apply(
                                    List.of(convertToMessage(pse)), enrichContextInformation(pse, batchContext)
                            )
                    );
                }
                return result;
            }).thenRun(() -> {
                if (logger.isTraceEnabled()) {
                    logger.trace("{}/{} processed {} entries",
                                 streamId, persistentStreamSegment.segment(), batch.size());
                }
                persistentStreamSegment.acknowledge(token);
            });
        }

        public void messageAvailable() {
            // closing a segment appends its terminal marker, which also triggers this callback — don't drop
            // that notification, or an idle segment's drain never gets scheduled and close() stalls on it
            if (!processGate.get() && (opened.get() || persistentStreamSegment.isClosed())) {
                scheduler.submit(this::readMessagesFromSegment);
            }
        }

        private void readMessagesFromSegment() {
            currentState.get().readMessages();
        }

        private EventMessage convertToMessage(PersistentStreamEvent pse) {
            Event event = pse.getEvent().getEvent();
            SerializedObject payload = event.getPayload();
            return new GenericEventMessage(
                    event.getMessageIdentifier(),
                    eventTypeResolver.resolve(payload.getType(), payload.getRevision()),
                    payload.getData().toByteArray(),
                    new Metadata(MetadataConverter.convertMetadataValuesToGrpc(event.getMetaDataMap())),
                    Instant.ofEpochMilli(event.getTimestamp())
            ).withConverter(converter);
        }

        // Branches off the batch context per event rather than writing into it, so an event only ever sees the
        // information of its own, and nothing an event is given outlives the event it belongs to.
        private ProcessingContext enrichContextInformation(PersistentStreamEvent pse,
                                                           ProcessingContext batchContext) {
            // supply tracking information
            ProcessingContext eventContext = batchContext.withResource(TrackingToken.RESOURCE_KEY, createToken(pse));

            String aggregateIdentifier = getAggregateIdentifier(pse);
            if (aggregateIdentifier != null && !aggregateIdentifier.isEmpty()) {
                // supply legacy aggregate information
                eventContext = eventContext.withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY,
                                                         aggregateIdentifier);
                String aggregateType = getAggregateType(pse);
                if (aggregateType != null) {
                    eventContext = eventContext.withResource(LegacyResources.AGGREGATE_TYPE_KEY, aggregateType);
                }
                eventContext = eventContext.withResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY,
                                                         getAggregateSequenceNumber(pse));
            }
            return eventContext;
        }

        private TrackingToken createToken(PersistentStreamEvent event) {
            if (!event.getReplay()) {
                return new GlobalSequenceTrackingToken(event.getEvent().getToken());
            }
            // this emulates a replay from the next event at the moment for every replay event received,
            // since the persistent streams API does not expose the token at reset
            return ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(event.getEvent().getToken() + 1),
                                                 new GlobalSequenceTrackingToken(event.getEvent().getToken()));
        }

        private @Nullable String getAggregateIdentifier(PersistentStreamEvent pse) {
            String aggregateIdentifier = pse.getEvent().getEvent().getAggregateIdentifier();
            return aggregateIdentifier.isEmpty() ? null : aggregateIdentifier;
        }

        private @Nullable String getAggregateType(PersistentStreamEvent pse) {
            String aggregateType = pse.getEvent().getEvent().getAggregateType();
            return aggregateType.isEmpty() ? null : aggregateType;
        }

        private long getAggregateSequenceNumber(PersistentStreamEvent pse) {
            return pse.getEvent().getEvent().getAggregateSequenceNumber();
        }
    }
}
