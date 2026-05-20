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
import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.MessageType;
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
import java.util.Optional;
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
 * Opens a gRPC connection to Axon Server via {@link #open(BiFunction)} and invokes the supplied
 * {@link BiFunction} for each batch. The batch consumer returns a {@link CompletableFuture} that must complete before
 * the token for the last event in the batch is acknowledged to Axon Server. On consumer failure the batch is retried
 * with exponential back-off.
 *
 * @author Marc Gathier
 * @author Jakob Hatzl
 * @since 4.10.0
 */
@Internal
public class PersistentStreamConnection {

    private static final int MAX_RETRY_INTERVAL_SECONDS = 60;
    private static final int MIN_RETRY_INTERVAL_SECONDS = 1;
    private final Logger logger = LoggerFactory.getLogger(PersistentStreamConnection.class);

    private final String streamId;
    private final AxonServerConnectionManager connectionManager;
    private final AxonServerConfiguration serverConfig;
    private final EventConverter converter;
    private final PersistentStreamProperties persistentStreamProperties;

    private final AtomicReference<@Nullable PersistentStream> persistentStreamHolder = new AtomicReference<>();

    private static final BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>>
            NO_OP_CONSUMER = (events, ctx) -> CompletableFuture.completedFuture(null);
    private final AtomicReference<BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>>>
            consumer = new AtomicReference<>(NO_OP_CONSUMER);

    private final ScheduledExecutorService scheduler;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final int batchSize;
    private final Map<Integer, SegmentConnection> segments = new ConcurrentHashMap<>();
    private final AtomicInteger retrySeconds = new AtomicInteger(MIN_RETRY_INTERVAL_SECONDS);

    private final @Nullable String defaultContext;

    /**
     * Instantiates a {@code PersistentStreamConnection}.
     *
     * @param streamId                   the unique identifier of the persistent stream
     * @param connectionManager          the Axon Server connection manager
     * @param serverConfig               the Axon Server configuration
     * @param converter                  the event converter used to deserialize event payloads
     * @param persistentStreamProperties the properties for the persistent stream
     * @param scheduler                  the scheduler thread pool to schedule tasks
     * @param batchSize                  the maximum number of events to collect per batch
     */
    public PersistentStreamConnection(String streamId,
                                      AxonServerConnectionManager connectionManager,
                                      AxonServerConfiguration serverConfig,
                                      EventConverter converter,
                                      PersistentStreamProperties persistentStreamProperties,
                                      ScheduledExecutorService scheduler,
                                      UnitOfWorkFactory unitOfWorkFactory,
                                      int batchSize) {
        this(streamId,
             connectionManager,
             serverConfig,
             converter,
             persistentStreamProperties,
             scheduler,
             unitOfWorkFactory,
             batchSize,
             null);
    }

    /**
     * Instantiates a {@code PersistentStreamConnection}.
     *
     * @param streamId                   the unique identifier of the persistent stream
     * @param connectionManager          the Axon Server connection manager
     * @param serverConfig               the Axon Server configuration
     * @param converter                  the event converter used to deserialize event payloads
     * @param persistentStreamProperties the properties for the persistent stream
     * @param scheduler                  the scheduler thread pool to schedule tasks
     * @param batchSize                  the maximum number of events to collect per batch
     * @param defaultContext             the Axon Server context to connect to, or {@code null} to use
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
                                      @Nullable String defaultContext) {
        this.streamId = streamId;
        this.connectionManager = connectionManager;
        this.serverConfig = serverConfig;
        this.converter = converter;
        this.persistentStreamProperties = persistentStreamProperties;
        this.scheduler = scheduler;
        this.unitOfWorkFactory = unitOfWorkFactory;
        this.batchSize = batchSize;
        this.defaultContext = defaultContext;
    }

    /**
     * Initiates the connection to Axon Server and starts delivering events to the given {@code consumer}.
     * <p>
     * The stream can be opened with only a single consumer at a time.
     *
     * @param consumer the consumer of batches of event messages; receives each batch and a {@code null}
     *                 {@link ProcessingContext}, and must return a {@link CompletableFuture} that completes when the
     *                 batch has been processed
     * @throws IllegalStateException if the stream was already opened
     */
    public void open(BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> consumer) {
        if (!this.consumer.compareAndSet(NO_OP_CONSUMER, consumer)) {
            throw new IllegalStateException(
                    String.format("%s: Persistent Stream has already been opened.", streamId));
        }
        start();
    }

    private void start() {
        String context = defaultContext != null && !defaultContext.isEmpty()
                ? defaultContext
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
        retrySeconds.set(1);
        segments.put(persistentStreamSegment.segment(), new SegmentConnection(persistentStreamSegment));
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

    private void streamClosed(Throwable throwable) {
        persistentStreamHolder.set(null);
        if (throwable != null) {
            logger.info("{}: Rescheduling persistent stream", streamId, throwable);
            scheduler.schedule(this::start,
                               retrySeconds.getAndUpdate(current -> Math.min(MAX_RETRY_INTERVAL_SECONDS, current * 2)),
                               TimeUnit.SECONDS);
        }
    }

    /**
     * Closes the persistent stream connection to Axon Server.
     */
    public void close() {
        PersistentStream persistentStream = persistentStreamHolder.getAndSet(null);
        if (persistentStream != null) {
            persistentStream.close();
            this.consumer.set(NO_OP_CONSUMER);
        }
    }

    private interface SegmentState {

        void readMessages();
    }

    private class SegmentConnection {

        private final AtomicBoolean processGate = new AtomicBoolean();
        private final AtomicBoolean doneConfirmed = new AtomicBoolean();
        private final PersistentStreamSegment persistentStreamSegment;
        private final AtomicReference<SegmentState> currentState = new AtomicReference<>(new ProcessingState());

        public SegmentConnection(PersistentStreamSegment persistentStreamSegment) {
            this.persistentStreamSegment = persistentStreamSegment;
        }

        private class RetryState implements SegmentState {

            private final List<PersistentStreamEvent> batch;
            private final AtomicInteger retryInterval = new AtomicInteger(MIN_RETRY_INTERVAL_SECONDS);

            public RetryState(List<PersistentStreamEvent> batch) {
                this.batch = batch;
                scheduler.schedule(this::retry, retryInterval.get(), TimeUnit.SECONDS);
            }

            private void retry() {
                try {
                    processBatch(batch);
                    currentState.set(new ProcessingState());
                    scheduler.submit(SegmentConnection.this::readMessagesFromSegment);
                } catch (Exception ex) {
                    int interval = retryInterval.updateAndGet(old -> Math.min(old * 2, MAX_RETRY_INTERVAL_SECONDS));
                    logger.warn("{}: Exception while retrying events for segment {}, retrying after {} seconds",
                                streamId, persistentStreamSegment.segment(), interval, ex);
                    scheduler.schedule(this::retry, interval, TimeUnit.SECONDS);
                }
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

                try {
                    if (!persistentStreamSegment.isClosed()) {
                        List<PersistentStreamEvent> batch = readBatch(persistentStreamSegment);
                        if (!batch.isEmpty()) {
                            try {
                                processBatch(batch);
                            } catch (Exception ex) {
                                logger.warn(
                                        "{}: Exception while processing events for segment {}, retrying after {} second",
                                        streamId,
                                        persistentStreamSegment.segment(),
                                        MIN_RETRY_INTERVAL_SECONDS,
                                        ex);
                                currentState.set(new RetryState(batch));
                            }
                        }
                    }

                    acknowledgeDoneWhenClosed(persistentStreamSegment);
                } catch (StreamClosedException e) {
                    logger.debug("{}: Stream closed for segment {}", streamId, persistentStreamSegment.segment());
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    persistentStreamSegment.error(e.getMessage());
                    logger.warn("{}: Exception while processing events for segment {}",
                                streamId, persistentStreamSegment.segment(), e);
                } finally {
                    processGate.set(false);
                    if (!persistentStreamSegment.isClosed() && persistentStreamSegment.peek() != null) {
                        scheduler.submit(SegmentConnection.this::readMessagesFromSegment);
                    }
                }
            }

            private List<PersistentStreamEvent> readBatch(
                    PersistentStreamSegment persistentStreamSegment
            ) throws InterruptedException {
                List<PersistentStreamEvent> batch = new LinkedList<>();
                PersistentStreamEvent event = persistentStreamSegment.nextIfAvailable();
                if (event == null) {
                    return batch;
                }
                batch.add(event);
                while (batch.size() < batchSize && !persistentStreamSegment.isClosed()
                        && (event = persistentStreamSegment.nextIfAvailable(1, TimeUnit.MILLISECONDS)) != null) {
                    batch.add(event);
                }
                return batch;
            }

            private void acknowledgeDoneWhenClosed(PersistentStreamSegment persistentStreamSegment) {
                if (persistentStreamSegment.isClosed() && doneConfirmed.compareAndSet(false, true)) {
                    persistentStreamSegment.acknowledge(PENDING_WORK_DONE_MARKER);
                }
            }
        }

        private void processBatch(List<PersistentStreamEvent> batch) {
            if (!persistentStreamSegment.isClosed()) {
                PersistentStreamEvent batchLastEvent = batch.getLast();
                long token = batchLastEvent.getEvent().getToken();
                TrackingToken batchEndToken = createToken(batchLastEvent);
                UnitOfWork unitOfWork = unitOfWorkFactory.create();
                // TODO omit joinAndUnwrap here and use allOf instead
                FutureUtils.joinAndUnwrap(unitOfWork.executeWithResult(processingContext -> {
                    CompletableFuture<?> result = CompletableFuture.completedFuture(null);
                    processingContext.putResource(TrackingToken.BATCH_END_RESOURCE_KEY, batchEndToken);
                    for (PersistentStreamEvent pse : batch) {
                        result = result
                                .thenCompose(ignored ->
                                                     consumer.get().apply(List.of(convertToMessage(pse)),
                                                                          enrichContextInformation(pse,
                                                                                                   processingContext)));
                    }
                    return result;
                }));
                if (logger.isTraceEnabled()) {
                    logger.trace("{}/{} processed {} entries",
                                 streamId,
                                 persistentStreamSegment.segment(),
                                 batch.size());
                }
                persistentStreamSegment.acknowledge(token);
            }
        }

        public void messageAvailable() {
            if (!processGate.get()) {
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
                    new MessageType(payload.getType(), payload.getRevision()),
                    payload.getData().toByteArray(),
                    new Metadata(MetadataConverter.convertMetadataValuesToGrpc(event.getMetaDataMap())),
                    Instant.ofEpochMilli(event.getTimestamp())
            ).withConverter(converter);
        }

        private ProcessingContext enrichContextInformation(PersistentStreamEvent pse,
                                                           ProcessingContext processingContext) {
            // supply tracking information
            TrackingToken token = createToken(pse);
            processingContext.putResource(TrackingToken.RESOURCE_KEY, token);

            Optional<String> aggregateIdentifier = getAggregateIdentifier(pse);
            aggregateIdentifier.ifPresentOrElse(aggregateId -> {
                // supply legacy aggregate information
                processingContext.putResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, aggregateId);
                String aggregateType = getAggregateType(pse);
                if (aggregateType != null) {
                    processingContext.putResource(LegacyResources.AGGREGATE_TYPE_KEY, aggregateType);
                }
                processingContext.putResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY,
                                              getAggregateSequenceNumber(pse));
            }, () -> {
                // reset legacy aggregate information in case no aggregateId is present
                processingContext.removeResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY);
                processingContext.removeResource(LegacyResources.AGGREGATE_TYPE_KEY);
                processingContext.removeResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY);
            });
            return processingContext;
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

        private Optional<String> getAggregateIdentifier(PersistentStreamEvent pse) {
            String aggregateIdentifier = pse.getEvent().getEvent().getAggregateIdentifier();
            return aggregateIdentifier.isEmpty() ? Optional.empty() : Optional.of(aggregateIdentifier);
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
