/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests the {@link WaitAssociationIndex}: how rows enter and leave it, which events they match, and how it tracks the
 * segments this node holds.
 *
 * @author Stefan Dragisic
 */
class WaitAssociationIndexTest {

    private static final QualifiedName PAYMENT_RECEIVED = new QualifiedName("PaymentReceived");
    private static final Segment FIRST_HALF = new Segment(0, 1);
    private static final Segment SECOND_HALF = new Segment(1, 1);
    private static final ProcessingContext CONTEXT = new StubProcessingContext();

    private final Set<String> members = ConcurrentHashMap.newKeySet();
    private WaitAssociationIndex index;

    private static EventMessage paymentReceived(Map<String, Object> payload) {
        return new GenericEventMessage(new MessageType(PAYMENT_RECEIVED), payload);
    }

    private static EventMessage paymentReceived(Map<String, Object> payload, Metadata metadata) {
        return new GenericEventMessage(new MessageType(PAYMENT_RECEIVED), payload, metadata);
    }

    private static EventMessage waitStarted(String workflowId, String stepName, Set<String> associations) {
        return new GenericEventMessage(
                new MessageType(new QualifiedName("AwaitStarted")),
                Map.of("eventName", PAYMENT_RECEIVED.toString(), "associations", associations),
                MetadataUtils.markWaitForEventStep(MetadataUtils.create(workflowId, stepName, StepStatus.STARTED))
        );
    }

    private static EventMessage stepEnded(String workflowId, String stepName) {
        return new GenericEventMessage(new MessageType(new QualifiedName("AwaitCompleted")),
                                       Map.of(),
                                       MetadataUtils.create(workflowId, stepName, StepStatus.COMPLETED));
    }

    private static EventMessage workflowEnded(String workflowId) {
        return new GenericEventMessage(new MessageType(new QualifiedName("WorkflowCompleted")),
                                       Map.of(),
                                       MetadataUtils.create(workflowId, WorkflowStatus.COMPLETED));
    }

    private static WaitAssociation row(String workflowId, Set<String> associations) {
        return new WaitAssociation(workflowId, "awaitPayment", PAYMENT_RECEIVED, associations);
    }

    private static String idOwnedBy(Segment segment) {
        return IntStream.range(0, 1_000)
                        .mapToObj(index -> "workflow-" + index)
                        .filter(id -> WorkflowSegmentOwnership.ownedBy(segment, id))
                        .findFirst()
                        .orElseThrow();
    }

    @BeforeEach
    void setUp() {
        index = new WaitAssociationIndex(members::contains);
    }

    @Nested
    class Evolve {

        @Test
        void aStartedWaitOfAMemberIsAdded() {
            // given
            members.add("order-1");

            // when
            index.evolve(waitStarted("order-1", "awaitPayment", Set.of("payload:orderId=42")));

            // then
            assertThat(index.match(paymentReceived(Map.of("orderId", "42")), CONTEXT)).containsExactly("order-1");
        }

        @Test
        void aStartedWaitReplacesTheEarlierRowOfThatStep() {
            // given
            members.add("order-1");
            index.evolve(waitStarted("order-1", "awaitPayment", Set.of("payload:orderId=42")));

            // when
            index.evolve(waitStarted("order-1", "awaitPayment", Set.of("payload:orderId=43")));

            // then
            assertThat(index.match(paymentReceived(Map.of("orderId", "42")), CONTEXT)).isEmpty();
            assertThat(index.match(paymentReceived(Map.of("orderId", "43")), CONTEXT)).containsExactly("order-1");
        }

        @Test
        void aStartedWaitOfferedAgainStaysVisibleToConcurrentLookups() {
            // given
            members.add("order-1");
            index.evolve(waitStarted("order-1", "awaitPayment", Set.of("payload:orderId=42")));
            var lookup = paymentReceived(Map.of("orderId", "42"));

            // when: a claim behind the stream offers the same STARTED event again while a worker looks it up
            var reoffers = CompletableFuture.runAsync(() -> IntStream.range(0, 10_000).forEach(
                    i -> index.evolve(waitStarted("order-1", "awaitPayment", Set.of("payload:orderId=42")))
            ));

            // then
            while (!reoffers.isDone()) {
                assertThat(index.match(lookup, CONTEXT)).containsExactly("order-1");
            }
            reoffers.orTimeout(10, TimeUnit.SECONDS).join();
            assertThat(index.match(lookup, CONTEXT)).containsExactly("order-1");
        }

        @Test
        void aWaitMovedBetweenPlacementsStaysVisibleToConcurrentLookups() {
            // given
            members.add("order-1");
            index.evolve(waitStarted("order-1", "awaitPayment", Set.of("payload:orderId=42")));
            var lookup = paymentReceived(Map.of("orderId", "42"));

            // when: the row moves between its keyed and its name-only placement, both matching the lookup
            var moves = CompletableFuture.runAsync(() -> IntStream.range(0, 10_000).forEach(
                    i -> index.evolve(waitStarted("order-1", "awaitPayment",
                                                    i % 2 == 0 ? Set.of() : Set.of("payload:orderId=42")))
            ));

            // then
            while (!moves.isDone()) {
                assertThat(index.match(lookup, CONTEXT)).containsExactly("order-1");
            }
            moves.orTimeout(10, TimeUnit.SECONDS).join();
            assertThat(index.match(lookup, CONTEXT)).containsExactly("order-1");
        }

        @Test
        void aStartedWaitOfAWorkflowThatIsNoMemberIsIgnored() {
            // when
            index.evolve(waitStarted("order-1", "awaitPayment", Set.of("payload:orderId=42")));

            // then
            assertThat(index.match(paymentReceived(Map.of("orderId", "42")), CONTEXT)).isEmpty();
        }

        @Test
        void aTerminalStepRemovesTheRowOfThatStepOnly() {
            // given
            members.add("order-1");
            index.evolve(waitStarted("order-1", "awaitPayment", Set.of("payload:orderId=42")));
            index.evolve(waitStarted("order-1", "awaitRefund", Set.of("payload:orderId=42")));

            // when
            index.evolve(stepEnded("order-1", "awaitPayment"));

            // then: the other wait of the workflow still routes the event
            assertThat(index.match(paymentReceived(Map.of("orderId", "42")), CONTEXT)).containsExactly("order-1");

            // when
            index.evolve(stepEnded("order-1", "awaitRefund"));

            // then
            assertThat(index.match(paymentReceived(Map.of("orderId", "42")), CONTEXT)).isEmpty();
        }

        @Test
        void aTerminalWorkflowRemovesAllItsRows() {
            // given
            members.addAll(Set.of("order-1", "order-2"));
            index.evolve(waitStarted("order-1", "awaitPayment", Set.of()));
            index.evolve(waitStarted("order-1", "awaitRefund", Set.of()));
            index.evolve(waitStarted("order-2", "awaitPayment", Set.of()));

            // when
            index.evolve(workflowEnded("order-1"));

            // then
            assertThat(index.match(paymentReceived(Map.of()), CONTEXT)).containsExactly("order-2");
        }

        @Test
        void businessEventsChangeNothing() {
            // given
            index.segmentLoaded(FIRST_HALF, List.of(row("order-42", Set.of("payload:orderId=42"))));

            // when
            index.evolve(paymentReceived(Map.of("orderId", "42")));

            // then
            assertThat(index.match(paymentReceived(Map.of("orderId", "42")), CONTEXT)).containsExactly("order-42");
            assertThat(index.allClaimedSegmentsLoaded()).isTrue();
        }
    }

    @Nested
    class Match {

        @Test
        void anEqualityAssociationMatchesItsValueOnly() {
            // given
            index.segmentLoaded(FIRST_HALF, List.of(row("order-42", Set.of("payload:orderId=42")),
                                              row("order-43", Set.of("payload:orderId=43"))));

            // when / then
            assertThat(index.match(paymentReceived(Map.of("orderId", "42")), CONTEXT)).containsExactly("order-42");
            assertThat(index.match(paymentReceived(Map.of("orderId", 43)), CONTEXT))
                    .as("values compare as text, as the wait condition compares them")
                    .containsExactly("order-43");
            assertThat(index.match(paymentReceived(Map.of("orderId", "44")), CONTEXT)).isEmpty();
        }

        @Test
        void everyAssociationOfARowMustHold() {
            // given
            index.segmentLoaded(FIRST_HALF,
                          List.of(row("order-42", Set.of("payload:orderId=42", "metadata:tenant=acme"))));

            // when / then
            assertThat(index.match(paymentReceived(Map.of("orderId", "42"),
                                                   Metadata.emptyInstance().and("tenant", "acme")), CONTEXT))
                    .containsExactly("order-42");
            assertThat(index.match(paymentReceived(Map.of("orderId", "42"),
                                                   Metadata.emptyInstance().and("tenant", "other")), CONTEXT))
                    .isEmpty();
        }

        @Test
        void aRowWithoutAssociationsMatchesEveryEventWithItsName() {
            // given
            index.segmentLoaded(FIRST_HALF, List.of(row("order-42", Set.of())));

            // when / then
            assertThat(index.match(paymentReceived(Map.of("orderId", "anything")), CONTEXT))
                    .containsExactly("order-42");
            assertThat(index.match(new GenericEventMessage(new MessageType(new QualifiedName("OtherEvent")),
                                                           Map.of()), CONTEXT))
                    .isEmpty();
        }

        @Test
        void anAssociationThatIsNoPlainEqualityMatchesOnTheEventNameAlone() {
            // given: parsed without a ">=" operator, the path becomes "amount>", which no event carries
            index.segmentLoaded(FIRST_HALF, List.of(row("order-42", Set.of("payload:amount>=5"))));

            // when / then
            assertThat(index.match(paymentReceived(Map.of("amount", "1")), CONTEXT)).containsExactly("order-42");
        }

        @Test
        void aRowWithoutAnEventNameMatchesEveryEvent() {
            // given
            index.segmentLoaded(FIRST_HALF, List.of(new WaitAssociation("order-42", "awaitPayment", null, Set.of())));

            // when / then
            assertThat(index.match(new GenericEventMessage(new MessageType(new QualifiedName("AnyEvent")),
                                                           Map.of()), CONTEXT))
                    .containsExactly("order-42");
        }

        @Test
        void aSleepNeverMatchesABusinessEvent() {
            // given
            index.segmentLoaded(FIRST_HALF, List.of(new WaitAssociation(
                    "order-42", "sleep", new QualifiedName(Void.class), Set.of()
            )));

            // when / then
            assertThat(index.match(paymentReceived(Map.of()), CONTEXT)).isEmpty();
        }

        @Test
        void aMissingValueMatchesNothing() {
            // given
            index.segmentLoaded(FIRST_HALF, List.of(row("order-42", Set.of("payload:orderId=42"))));

            // when / then
            assertThat(index.match(paymentReceived(Map.of("other", "42")), CONTEXT)).isEmpty();
        }

        @Test
        void aPayloadThatCannotBeReadFailsTheLookup() {
            // given
            index.segmentLoaded(FIRST_HALF, List.of(row("order-42", Set.of("payload:orderId=42"))));
            var unreadable = new GenericEventMessage(new MessageType(PAYMENT_RECEIVED), "not a map");

            // when / then
            assertThatThrownBy(() -> index.match(unreadable, CONTEXT)).isInstanceOf(RuntimeException.class);
        }

        @Test
        void concurrentWritesLeaveNoRowBehind() {
            // given
            var workflowIds = IntStream.range(0, 200).mapToObj(i -> "order-" + i).toList();
            members.addAll(workflowIds);

            // when: every workflow opens and closes a wait, in parallel with lookups
            var writers = workflowIds.stream()
                                     .map(id -> CompletableFuture.runAsync(() -> {
                                         index.evolve(waitStarted(id, "awaitPayment",
                                                                    Set.of("payload:orderId=42")));
                                         index.match(paymentReceived(Map.of("orderId", "42")), CONTEXT);
                                         index.evolve(stepEnded(id, "awaitPayment"));
                                     }))
                                     .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(writers).orTimeout(10, TimeUnit.SECONDS).join();

            // then
            assertThat(index.match(paymentReceived(Map.of("orderId", "42")), CONTEXT)).isEmpty();
        }
    }

    @Nested
    class Segments {

        @Test
        void aClaimedSegmentIsLoadingUntilItsRowsAreLoaded() {
            // when
            index.segmentClaimed(FIRST_HALF);

            // then
            assertThat(index.allClaimedSegmentsLoaded()).isFalse();

            // when
            index.segmentLoaded(FIRST_HALF, List.of());

            // then
            assertThat(index.allClaimedSegmentsLoaded()).isTrue();
        }

        @Test
        void releasingASegmentDropsTheRowsOfTheWorkflowsItOwns() {
            // given
            var ownedByFirst = idOwnedBy(FIRST_HALF);
            var ownedBySecond = idOwnedBy(SECOND_HALF);
            index.segmentLoaded(FIRST_HALF, List.of(row(ownedByFirst, Set.of())));
            index.segmentLoaded(SECOND_HALF, List.of(row(ownedBySecond, Set.of())));

            // when
            index.segmentReleased(FIRST_HALF);

            // then
            assertThat(index.match(paymentReceived(Map.of()), CONTEXT)).containsExactly(ownedBySecond);
            assertThat(index.allClaimedSegmentsMatch(segment -> segment.equals(SECOND_HALF))).isTrue();
        }

        @Test
        void clearForgetsSegmentsAndRows() {
            // given
            index.segmentLoaded(FIRST_HALF, List.of(row("order-42", Set.of())));

            // when
            index.clear();

            // then
            assertThat(index.allClaimedSegmentsMatch(segment -> false)).isTrue();
            assertThat(index.match(paymentReceived(Map.of()), CONTEXT)).isEmpty();
        }
    }
}
