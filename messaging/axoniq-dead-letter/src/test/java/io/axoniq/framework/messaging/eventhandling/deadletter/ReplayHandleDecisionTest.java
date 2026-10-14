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

package io.axoniq.framework.messaging.eventhandling.deadletter;

import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.GenericDeadLetter;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link ReplayHandleDecision}.
 *
 * @author Steven van Beelen
 */
class ReplayHandleDecisionTest {

    private static final String SEQUENCE_ID = "sequence-id";

    @Nested
    class WhenPositionsUnknown {

        @Test
        void decidesRegularWhenReplayedEventHasNoTrackingToken() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            ProcessingContext context = new StubProcessingContext();
            List<DeadLetter<? extends EventMessage>> letters = List.of(head);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.REGULAR);
        }

        @Test
        void decidesRegularWhenHeadLetterHasNoTrackingToken() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null, Context.empty()
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(3L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.REGULAR);
        }

        @Test
        void decidesRegularWhenNeitherEventNorHeadLetterHasTrackingToken() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null, Context.empty()
            );
            ProcessingContext context = new StubProcessingContext();
            List<DeadLetter<? extends EventMessage>> letters = List.of(head);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.REGULAR);
        }

        @Test
        void decidesHandleWhenOnlyLettersBehindTheHeadHaveNoPosition() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            DeadLetter<EventMessage> unknown = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("unknown"), null, Context.empty()
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(3L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head, unknown);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.HANDLE);
        }
    }

    @Nested
    class WhenEventPrecedesHead {

        @Test
        void decidesHandle() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(3L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.HANDLE);
        }

        @Test
        void decidesHandleWhenPositionedJustBeforeTheHeadLetter() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(4L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.HANDLE);
        }

        @Test
        void decidesHandleWhenSequenceHoldsMultipleLetters() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            DeadLetter<EventMessage> second = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("second"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(6L))
            );
            DeadLetter<EventMessage> third = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("third"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(1L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head, second, third);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.HANDLE);
        }

        @Test
        void decidesHandleWhenHeadLetterWasEnqueuedDuringAReplay() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            TrackingToken headToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(5L));
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, headToken)
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(3L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.HANDLE);
        }
    }

    @Nested
    class WhenEventAlreadyPresent {

        @Test
        void decidesSkipWhenEventIsPositionedBeforeTheHeadLetter() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, event, null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(3L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.SKIP);
        }

        @Test
        void decidesSkipWhenEventIsPositionedBeforeALetterBehindTheHeadLetter() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            DeadLetter<EventMessage> present = new GenericDeadLetter<>(
                    SEQUENCE_ID, event, null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(3L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head, present);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.SKIP);
        }

        @Test
        void decidesSkipWhenPositionsAreUnknown() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, event, null, Context.empty()
            );
            ProcessingContext context = new StubProcessingContext();
            List<DeadLetter<? extends EventMessage>> letters = List.of(head);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.SKIP);
        }

        @Test
        void decidesSkipForTheHeadLetterItself() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, event, null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(5L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.SKIP);
        }

        @Test
        void decidesSkipForALetterBehindTheHeadLetter() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            DeadLetter<EventMessage> second = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("second"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(6L))
            );
            DeadLetter<EventMessage> present = new GenericDeadLetter<>(
                    SEQUENCE_ID, event, null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(9L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head, second, present);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.SKIP);
        }

        @Test
        void decidesSkipWhenPresentLetterWasEnqueuedDuringAReplay() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            TrackingToken presentToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(9L));
            DeadLetter<EventMessage> present = new GenericDeadLetter<>(
                    SEQUENCE_ID, event, null, Context.with(TrackingToken.RESOURCE_KEY, presentToken)
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(9L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head, present);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.SKIP);
        }

        @Test
        void matchesOnEventIdentifierInsteadOfPayload() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("same-payload");
            EventMessage otherEventWithSamePayload = EventTestUtils.asEventMessage("same-payload");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            DeadLetter<EventMessage> other = new GenericDeadLetter<>(
                    SEQUENCE_ID, otherEventWithSamePayload, null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(9L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head, other);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.ENQUEUE_IN_ORDER);
        }
    }

    @Nested
    class WhenEventNotYetPresent {

        @Test
        void decidesEnqueueInOrderWhenPositionedAfterTheHeadLetter() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(7L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.ENQUEUE_IN_ORDER);
        }

        @Test
        void decidesEnqueueInOrderWhenPositionedAtTheHeadLetter() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(5L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.ENQUEUE_IN_ORDER);
        }

        @Test
        void comparesWithTheHeadLetterOnlyWhenSequenceHoldsMultipleLetters() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            DeadLetter<EventMessage> later = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("later"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(7L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head, later);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.ENQUEUE_IN_ORDER);
        }

        @Test
        void decidesEnqueueInOrderWhenPositionedBeyondAllLetters() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
            );
            DeadLetter<EventMessage> later = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("later"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(50L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head, later);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.ENQUEUE_IN_ORDER);
        }

        @Test
        void decidesEnqueueInOrderWhenHeadLetterWasEnqueuedDuringAReplay() {
            // given
            EventMessage event = EventTestUtils.asEventMessage("event");
            TrackingToken headToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(5L));
            DeadLetter<EventMessage> head = new GenericDeadLetter<>(
                    SEQUENCE_ID, EventTestUtils.asEventMessage("head"), null,
                    Context.with(TrackingToken.RESOURCE_KEY, headToken)
            );
            TrackingToken eventToken =
                    ((ReplayToken) ReplayToken.createReplayToken(new GlobalSequenceTrackingToken(100L)))
                            .advancedTo(new GlobalSequenceTrackingToken(7L));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TrackingToken.RESOURCE_KEY, eventToken);
            List<DeadLetter<? extends EventMessage>> letters = List.of(head);

            // when
            ReplayHandleDecision result = ReplayHandleDecision.decide(event, context, letters);

            // then
            assertThat(result).isEqualTo(ReplayHandleDecision.ENQUEUE_IN_ORDER);
        }
    }
}
