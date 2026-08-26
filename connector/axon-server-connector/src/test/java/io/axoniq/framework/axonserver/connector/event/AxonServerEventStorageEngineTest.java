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
import io.axoniq.axonserver.grpc.event.dcb.Event;
import io.axoniq.axonserver.grpc.event.dcb.SequencedEvent;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsResponse;
import io.grpc.Status;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.messaging.core.FluxUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.junit.jupiter.api.*;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link AxonServerEventStorageEngine} through mocking.
 * <p>
 * For an integration tests, use the {@link AxonServerEventStorageEngineIT} instead.
 *
 * @author Steven van Beelen
 */
class AxonServerEventStorageEngineTest {

    private static final String EVENT_NAME = "test-event";

    private ResultStream<SourceEventsResponse> sourcingStream;
    private DcbEventChannel dcbEventChannel;

    private AxonServerEventStorageEngine testSubject;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        AxonServerConnection connection = mock(AxonServerConnection.class);
        dcbEventChannel = mock(DcbEventChannel.class);
        sourcingStream = mock(ResultStream.class);

        when(connection.dcbEventChannel()).thenReturn(dcbEventChannel);
        when(dcbEventChannel.source(any(SourceEventsRequest.class))).thenReturn(sourcingStream);
        when(sourcingStream.getError()).thenReturn(java.util.Optional.empty());
        when(sourcingStream.isClosed()).thenReturn(true);

        testSubject = new AxonServerEventStorageEngine(
                connection,
                new DelegatingEventConverter(new JacksonConverter())
        );
    }

    @Test
    void defaultResolverSubstitutesEmptyVersionWithMissingVersionDefault() {
        // given
        Event storedEvent = Event.newBuilder()
                                 .setIdentifier(UUID.randomUUID().toString())
                                 .setTimestamp(Instant.now().toEpochMilli())
                                 .setName(EVENT_NAME)
                                 // no setVersion() -- proto default is empty string
                                 .build();
        SourceEventsResponse eventResponse = SourceEventsResponse.newBuilder()
                                                                 .setEvent(SequencedEvent.newBuilder()
                                                                                         .setEvent(storedEvent)
                                                                                         .setSequence(0L)
                                                                                         .build())
                                                                 .build();
        SourceEventsResponse markerResponse = SourceEventsResponse.newBuilder()
                                                                  .setConsistencyMarker(0L)
                                                                  .build();
        when(sourcingStream.nextIfAvailable()).thenReturn(eventResponse, markerResponse, null);
        when(sourcingStream.peek()).thenReturn(eventResponse, markerResponse, null);
        SourcingCondition condition = SourcingCondition.conditionFor(
                EventCriteria.havingTags("AGGREGATE_TYPE", UUID.randomUUID().toString())
        );
        // when / then
        StepVerifier.create(FluxUtils.of(testSubject.source(condition)))
                    .assertNext(entry -> {
                        assertThat(entry.message()).isNotInstanceOf(TerminalEventMessage.class);
                        assertThat(entry.message().type().name()).isEqualTo(EVENT_NAME);
                        assertThat(entry.message().type().version())
                                .isEqualTo(EventTypeResolver.MISSING_VERSION_DEFAULT);
                    })
                    .expectNextMatches(entry -> entry.message() instanceof TerminalEventMessage)
                    .verifyComplete();
    }

    @Nested
    class CommitFailures {

        private DcbEventChannel.AppendEventsTransaction serverTransaction;

        @BeforeEach
        void setUp() {
            serverTransaction = mock(DcbEventChannel.AppendEventsTransaction.class);
            when(dcbEventChannel.startTransaction(any())).thenReturn(serverTransaction);
        }

        @Test
        void conditionNotMetIsReportedAsAConsistencyRejection() {
            // given a commit failing the way Axon Server reports an unmet append condition
            Throwable serverFailure = Status.CANCELLED
                    .withDescription("io.axoniq.axonserver.eventstore.api.ConsistencyConditionException: "
                                             + "Consistency condition is not met.")
                    .asRuntimeException();
            when(serverTransaction.commit()).thenReturn(CompletableFuture.failedFuture(serverFailure));

            // when
            CompletableFuture<AppendEventsResponse> result = commit();

            // then
            assertThatThrownBy(() -> result.orTimeout(5, TimeUnit.SECONDS).join())
                    .hasCauseInstanceOf(AppendEventsTransactionRejectedException.class)
                    .rootCause()
                    .isSameAs(serverFailure);
        }

        @Test
        void aBrokenConnectionIsNotReportedAsAConsistencyRejection() {
            // given a commit failing because the connection to Axon Server is gone, so nobody knows the outcome
            Throwable transportFailure = Status.UNAVAILABLE
                    .withDescription("io exception")
                    .asRuntimeException();
            when(serverTransaction.commit()).thenReturn(CompletableFuture.failedFuture(transportFailure));

            // when
            CompletableFuture<AppendEventsResponse> result = commit();

            // then
            assertThatThrownBy(() -> result.orTimeout(5, TimeUnit.SECONDS).join())
                    .cause()
                    .isNotInstanceOf(AppendEventsTransactionRejectedException.class)
                    .isInstanceOf(EventStoreException.class)
                    .hasRootCause(transportFailure);
        }

        @Test
        void aCancelledCommitWithoutAServerDecisionIsNotReportedAsAConsistencyRejection() {
            // given a commit cancelled without Axon Server reporting a decision, the status it also uses
            // for an unmet append condition
            Throwable cancellation = Status.CANCELLED.withDescription("cancelled before receiving half close")
                                                     .asRuntimeException();
            when(serverTransaction.commit()).thenReturn(CompletableFuture.failedFuture(cancellation));

            // when
            CompletableFuture<AppendEventsResponse> result = commit();

            // then
            assertThatThrownBy(() -> result.orTimeout(5, TimeUnit.SECONDS).join())
                    .cause()
                    .isNotInstanceOf(AppendEventsTransactionRejectedException.class)
                    .isInstanceOf(EventStoreException.class)
                    .hasRootCause(cancellation);
        }

        @Test
        void conditionNotMetIsRecognisedWhenReportedDeeperInTheCauseChain() {
            // given the same rejection, wrapped by a layer that reports it as the cause rather than throwing it itself
            Throwable serverFailure = Status.CANCELLED
                    .withDescription("io.axoniq.axonserver.eventstore.api.ConsistencyConditionException: "
                                             + "Consistency condition is not met.")
                    .asRuntimeException();
            Throwable wrapped = new IllegalStateException("Append transaction failed.",
                                                          new IllegalStateException("Commit failed.", serverFailure));
            when(serverTransaction.commit()).thenReturn(CompletableFuture.failedFuture(wrapped));

            // when
            CompletableFuture<AppendEventsResponse> result = commit();

            // then the rejection is still recognised, two causes down
            assertThatThrownBy(() -> result.orTimeout(5, TimeUnit.SECONDS).join())
                    .hasCauseInstanceOf(AppendEventsTransactionRejectedException.class)
                    .rootCause()
                    .isSameAs(serverFailure);
        }

        @Test
        void aTransportFailureCarryingAnEarlierRejectionAsItsCauseIsNotReportedAsAConsistencyRejection() {
            // given a lost connection whose cause chain happens to carry an earlier rejection, so the marker is
            // present even though this append was never decided on
            Throwable earlierRejection = Status.CANCELLED
                    .withDescription("io.axoniq.axonserver.eventstore.api.ConsistencyConditionException: "
                                             + "Consistency condition is not met.")
                    .asRuntimeException();
            Throwable transportFailure = Status.UNAVAILABLE.withDescription("io exception")
                                                           .withCause(earlierRejection)
                                                           .asRuntimeException();
            when(serverTransaction.commit()).thenReturn(CompletableFuture.failedFuture(transportFailure));

            // when
            CompletableFuture<AppendEventsResponse> result = commit();

            // then the failure that terminated the call decides the outcome, so it is undetermined
            assertThatThrownBy(() -> result.orTimeout(5, TimeUnit.SECONDS).join())
                    .cause()
                    .isNotInstanceOf(AppendEventsTransactionRejectedException.class)
                    .isInstanceOf(EventStoreException.class);
        }

        @SuppressWarnings("unchecked")
        private CompletableFuture<AppendEventsResponse> commit() {
            EventMessage event = new GenericEventMessage(new MessageType(EVENT_NAME, "0.0.1"), "payload");
            EventStorageEngine.AppendTransaction<?> transaction = testSubject
                    .appendEvents(AppendCondition.none(),
                                  null,
                                  List.of(new GenericTaggedEventMessage<>(event, Set.of())))
                    .join();
            return (CompletableFuture<AppendEventsResponse>) transaction.commit();
        }
    }
}
