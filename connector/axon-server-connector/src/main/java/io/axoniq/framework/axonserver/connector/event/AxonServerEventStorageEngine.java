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

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.event.DcbEventChannel;
import io.axoniq.axonserver.grpc.event.dcb.AppendEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsResponse;
import io.grpc.Status;
import org.axonframework.common.ExceptionUtils;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EmptyAppendTransaction;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * An {@link EventStorageEngine} implementation using Axon Server through the {@code axonserver-connector-java}
 * project.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
public class AxonServerEventStorageEngine implements EventStorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final AxonServerConnection connection;
    private final TaggedEventConverter converter;

    /**
     * Constructs an {@code AxonServerEventStorageEngine} with the given {@code connection} and {@code converter}, using
     * {@link EventTypeResolver#DEFAULT} to resolve {@link org.axonframework.messaging.core.MessageType MessageTypes}
     * when reading events back from Axon Server.
     *
     * @param connection the context-specific backing connection to Axon Server
     * @param converter  the converter to use to serialize {@link EventMessage#payload() payloads} and complex
     *                   {@link Metadata} values into bytes
     */
    public AxonServerEventStorageEngine(AxonServerConnection connection,
                                        EventConverter converter) {
        this(connection, converter, EventTypeResolver.DEFAULT);
    }

    /**
     * Constructs an {@code AxonServerEventStorageEngine} with the given {@code connection}, {@code converter}, and
     * {@code eventTypeResolver}.
     *
     * @param connection        the context-specific backing connection to Axon Server
     * @param converter         the converter to use to serialize {@link EventMessage#payload() payloads} and complex
     *                          {@link Metadata} values into bytes
     * @param eventTypeResolver the resolver used to construct a
     *                          {@link org.axonframework.messaging.core.MessageType MessageType} from the event name and
     *                          version stored in Axon Server, handling missing or empty versions for legacy events
     */
    public AxonServerEventStorageEngine(AxonServerConnection connection,
                                        EventConverter converter,
                                        EventTypeResolver eventTypeResolver) {
        this.connection = Objects.requireNonNull(connection, "The Axon Server connection cannot be null.");
        this.converter = new TaggedEventConverter(converter, eventTypeResolver);
    }

    @Override
    public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                @Nullable ProcessingContext context,
                                                                List<TaggedEventMessage<?>> events) {
        if (events.isEmpty()) {
            return CompletableFuture.completedFuture(EmptyAppendTransaction.INSTANCE);
        }

        DcbEventChannel.AppendEventsTransaction appendTransaction =
                eventChannel().startTransaction(ConditionConverter.convertAppendCondition(condition));
        events.stream()
              .map(converter::convertTaggedEventMessage)
              .forEach(taggedEvent -> {
                  if (logger.isDebugEnabled()) {
                      logger.debug("Appended event [{}] with timestamp [{}].",
                                   taggedEvent.getEvent().getIdentifier(),
                                   taggedEvent.getEvent().getTimestamp());
                  }
                  appendTransaction.append(taggedEvent);
              });

        return CompletableFuture.completedFuture(new AxonServerAppendTransaction(appendTransaction, condition));
    }

    @Override
    public MessageStream<EventMessage> source(SourcingCondition condition, @Nullable ProcessingContext context) {
        if (logger.isDebugEnabled()) {
            logger.debug("Start sourcing events with condition [{}].", condition);
        }

        SourceEventsRequest sourcingRequest = ConditionConverter.convertSourcingCondition(condition);
        ResultStream<SourceEventsResponse> sourcingStream = eventChannel().source(sourcingRequest);
        return new SourcingEventMessageStream(sourcingStream, converter);
    }

    @Override
    public MessageStream<EventMessage> stream(StreamingCondition condition) {
        if (logger.isDebugEnabled()) {
            logger.debug("Start streaming events with condition [{}].", condition);
        }

        StreamEventsRequest streamingRequest = ConditionConverter.convertStreamingCondition(condition);
        ResultStream<StreamEventsResponse> stream = eventChannel().stream(streamingRequest);
        return new StreamingEventMessageStream(stream, converter);
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken() {
        if (logger.isDebugEnabled()) {
            logger.debug("Operation firstToken() is invoked.");
        }

        return eventChannel().tail()
                             .thenApply(response -> new GlobalSequenceTrackingToken(response.getSequence()));
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken() {
        if (logger.isDebugEnabled()) {
            logger.debug("Operation latestToken() is invoked.");
        }

        return eventChannel().head()
                             .thenApply(response -> new GlobalSequenceTrackingToken(response.getSequence()));
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at) {
        if (logger.isDebugEnabled()) {
            logger.debug("Operation tokenAt() is invoked with Instant [{}].", at);
        }

        return eventChannel().getSequenceAt(at)
                             .thenApply(response -> new GlobalSequenceTrackingToken(response.getSequence()));
    }

    private DcbEventChannel eventChannel() {
        return connection.dcbEventChannel();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("connection", connection);
        descriptor.describeProperty("converter", converter);
    }

    private record AxonServerAppendTransaction(
            DcbEventChannel.AppendEventsTransaction appendTransaction,
            AppendCondition condition
    ) implements AppendTransaction<AppendEventsResponse> {

        /**
         * The gRPC status Axon Server closes the append call with when the {@link AppendCondition} of this transaction
         * was not met. It is the same status a cancelled or broken call produces, so it narrows the set of candidate
         * failures without identifying a rejection on its own.
         */
        private static final Status.Code CONSISTENCY_CONDITION_NOT_MET_STATUS = Status.Code.CANCELLED;

        /**
         * Marker within the reported failure description that, together with
         * {@link #CONSISTENCY_CONDITION_NOT_MET_STATUS}, identifies an unmet {@link AppendCondition}. It is the name of
         * a server-side class, which is a weak contract: were Axon Server to rename that class, translate the
         * description, or close the call with a different status, a rejection would no longer be recognised and would
         * be reported as undetermined instead. That is the safe direction to be wrong in - the caller is told to
         * establish the outcome rather than being told a decision that was never made - but it makes this check worth
         * replacing as soon as Axon Server reports the outcome in a dedicated field or error code.
         */
        private static final String CONSISTENCY_CONDITION_FAILURE = "ConsistencyConditionException";

        /**
         * Commits this transaction, mapping a failure to the exception describing the outcome it really represents.
         * <p>
         * A failure reporting that the {@link AppendCondition} was not met is a decision by Axon Server not to append,
         * and surfaces as an {@link AppendEventsTransactionRejectedException}. Any other failure leaves the outcome
         * undetermined: the events may or may not have been appended, and it surfaces as an {@link EventStoreException}
         * instead. Both carry the original failure as their cause.
         *
         * @return a {@code CompletableFuture} of the {@link AppendEventsResponse}, failing with an
         * {@link AppendEventsTransactionRejectedException} when Axon Server decided against the append operation, and
         * with an {@link EventStoreException} when the outcome of the append operation is undetermined
         */
        @Override
        public CompletableFuture<AppendEventsResponse> commit() {
            logger.debug("Committing append event transaction...");
            return appendTransaction.commit()
                                    .exceptionallyCompose(failure -> mapCommitFailure(failure, condition));
        }

        private static CompletableFuture<AppendEventsResponse> mapCommitFailure(Throwable failure,
                                                                                AppendCondition condition) {
            if (isConsistencyConditionFailure(failure)) {
                Set<Tag> tags = condition.criteria()
                                         .flatten()
                                         .stream()
                                         .flatMap(criterion -> criterion.tags().stream())
                                         .collect(Collectors.toSet());

                AppendEventsTransactionRejectedException rejection =
                        new AppendEventsTransactionRejectedException(failure.getMessage(), tags);
                rejection.initCause(failure);
                return CompletableFuture.failedFuture(rejection);
            }
            return CompletableFuture.failedFuture(new EventStoreException(
                    "Failed committing events to Axon Server with exception [" + failure.getClass() + "].", failure
            ));
        }

        /**
         * Tells whether the given {@code failure} reports that the {@link AppendCondition} was not met.
         * <p>
         * Requires both signals Axon Server gives for that outcome: the status the append call was closed with, and the
         * marker in the reported description. Demanding the status as well as the marker keeps a failure that merely
         * carries an earlier rejection somewhere in its cause chain (a transport failure closing a call that had
         * already been refused) from reading as a rejection of this append, because
         * {@link Status#fromThrowable(Throwable)} resolves the status of the outermost failure in the chain, which is
         * the one that actually terminated the call.
         *
         * @param failure the failure the append commit completed with
         * @return {@code true} when {@code failure} reports an unmet {@code AppendCondition}, {@code false} otherwise
         */
        private static boolean isConsistencyConditionFailure(Throwable failure) {
            return Status.fromThrowable(failure).getCode()
                    == CONSISTENCY_CONDITION_NOT_MET_STATUS
                    && ExceptionUtils.findException(
                                             failure,
                                             cause -> cause.getMessage() != null
                                                     && cause.getMessage().contains(CONSISTENCY_CONDITION_FAILURE))
                                     .isPresent();
        }

        @Override
        public CompletableFuture<ConsistencyMarker> afterCommit(AppendEventsResponse appendResponse) {
            long marker = appendResponse.getConsistencyMarker();
            logger.debug("Committing append transaction succeeded with marker [{}].", marker);

            return CompletableFuture.completedFuture(new GlobalIndexConsistencyMarker(marker));
        }

        @Override
        public void rollback() {
            appendTransaction.rollback();
        }
    }
}
