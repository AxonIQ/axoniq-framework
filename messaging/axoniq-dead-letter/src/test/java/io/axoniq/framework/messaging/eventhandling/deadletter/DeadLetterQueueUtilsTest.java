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
import io.axoniq.framework.messaging.deadletter.InMemorySequencedDeadLetterQueue;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Test class validating {@link DeadLetterQueueUtils#enqueueInOrder}.
 *
 * @author Steven van Beelen
 */
class DeadLetterQueueUtilsTest {

    private static final String SEQUENCE_ID = "sequence-id";
    private static final String OTHER_SEQUENCE_ID = "other-sequence-id";

    private SequencedDeadLetterQueue<EventMessage> queue;
    private ProcessingContext context;

    @BeforeEach
    void setUp() {
        queue = InMemorySequencedDeadLetterQueue.<EventMessage>builder().build();
        context = new StubProcessingContext();
    }

    @Test
    void enqueuesInEmptySequence() {
        // given
        DeadLetter<EventMessage> newLetter = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("new"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
        );

        // when
        DeadLetterQueueUtils.enqueueInOrder(queue, SEQUENCE_ID, newLetter, context)
                            .orTimeout(2, TimeUnit.MILLISECONDS)
                            .join();

        // then
        assertThat(
                queue.deadLetterSequence(SEQUENCE_ID, null)
                     .orTimeout(2, TimeUnit.MILLISECONDS)
                     .join()
        ).extracting(letter -> letter.message().payload())
         .containsExactly("new");
    }

    @Test
    void enqueuesAtTheTailWhenPositionedAfterAllLetters() {
        // given
        DeadLetter<EventMessage> first = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("first"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(1L))
        );
        queue.enqueue(SEQUENCE_ID, first, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> second = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("second"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(2L))
        );
        queue.enqueue(SEQUENCE_ID, second, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> newLetter = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("new"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(3L))
        );

        // when
        DeadLetterQueueUtils.enqueueInOrder(queue, SEQUENCE_ID, newLetter, context)
                            .orTimeout(2, TimeUnit.MILLISECONDS)
                            .join();

        // then
        assertThat(
                queue.deadLetterSequence(SEQUENCE_ID, null)
                     .orTimeout(2, TimeUnit.MILLISECONDS)
                     .join()
        ).extracting(letter -> letter.message().payload())
         .containsExactly("first", "second", "new");
    }

    @Test
    void enqueuesAtTheHeadWhenPositionedBeforeAllLetters() {
        // given
        DeadLetter<EventMessage> first = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("first"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
        );
        queue.enqueue(SEQUENCE_ID, first, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> second = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("second"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(6L))
        );
        queue.enqueue(SEQUENCE_ID, second, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> third = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("third"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(7L))
        );
        queue.enqueue(SEQUENCE_ID, third, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> newLetter = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("new"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(1L))
        );

        // when
        DeadLetterQueueUtils.enqueueInOrder(queue, SEQUENCE_ID, newLetter, context)
                            .orTimeout(2, TimeUnit.MILLISECONDS)
                            .join();

        // then
        assertThat(
                queue.deadLetterSequence(SEQUENCE_ID, null)
                     .orTimeout(2, TimeUnit.MILLISECONDS)
                     .join()
        ).extracting(letter -> letter.message().payload())
         .containsExactly("new", "first", "second", "third");
    }

    @Test
    void enqueuesBetweenLettersWhenPositionedInTheMiddle() {
        // given
        DeadLetter<EventMessage> first = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("first"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(1L))
        );
        queue.enqueue(SEQUENCE_ID, first, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> second = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("second"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
        );
        queue.enqueue(SEQUENCE_ID, second, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> third = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("third"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
        );
        queue.enqueue(SEQUENCE_ID, third, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> newLetter = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("new"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(7L))
        );

        // when
        DeadLetterQueueUtils.enqueueInOrder(queue, SEQUENCE_ID, newLetter, context)
                            .orTimeout(2, TimeUnit.MILLISECONDS)
                            .join();

        // then
        assertThat(
                queue.deadLetterSequence(SEQUENCE_ID, null)
                     .orTimeout(2, TimeUnit.MILLISECONDS)
                     .join()
        ).extracting(letter -> letter.message().payload())
         .containsExactly("first", "second", "new", "third");
    }

    @Test
    void enqueuesBehindLettersWithTheSamePosition() {
        // given
        DeadLetter<EventMessage> first = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("first"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
        );
        queue.enqueue(SEQUENCE_ID, first, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> second = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("second"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
        );
        queue.enqueue(SEQUENCE_ID, second, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> third = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("third"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
        );
        queue.enqueue(SEQUENCE_ID, third, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> newLetter = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("new"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
        );

        // when
        DeadLetterQueueUtils.enqueueInOrder(queue, SEQUENCE_ID, newLetter, context)
                            .orTimeout(2, TimeUnit.MILLISECONDS)
                            .join();

        // then
        assertThat(
                queue.deadLetterSequence(SEQUENCE_ID, null)
                     .orTimeout(2, TimeUnit.MILLISECONDS)
                     .join()
        ).extracting(letter -> letter.message().payload())
         .containsExactly("first", "second", "new", "third");
    }

    @Test
    void keepsLettersWithoutPositionInFrontOfTheInsertionPointInPlace() {
        // given
        DeadLetter<EventMessage> first = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("first"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
        );
        queue.enqueue(SEQUENCE_ID, first, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> unknown = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("unknown"), null,
                Context.empty()
        );
        queue.enqueue(SEQUENCE_ID, unknown, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> third = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("third"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
        );
        queue.enqueue(SEQUENCE_ID, third, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> newLetter = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("new"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(7L))
        );

        // when
        DeadLetterQueueUtils.enqueueInOrder(queue, SEQUENCE_ID, newLetter, context)
                            .orTimeout(2, TimeUnit.MILLISECONDS)
                            .join();

        // then
        assertThat(
                queue.deadLetterSequence(SEQUENCE_ID, null)
                     .orTimeout(2, TimeUnit.MILLISECONDS)
                     .join()
        ).extracting(letter -> letter.message().payload())
         .containsExactly("first", "unknown", "new", "third");
    }

    @Test
    void movesLettersWithoutPositionBehindTheInsertionPoint() {
        // given
        DeadLetter<EventMessage> first = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("first"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
        );
        queue.enqueue(SEQUENCE_ID, first, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> third = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("third"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
        );
        queue.enqueue(SEQUENCE_ID, third, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> unknown = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("unknown"), null, Context.empty()
        );
        queue.enqueue(SEQUENCE_ID, unknown, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> newLetter = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("new"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(7L))
        );

        // when
        DeadLetterQueueUtils.enqueueInOrder(queue, SEQUENCE_ID, newLetter, context)
                            .orTimeout(2, TimeUnit.MILLISECONDS)
                            .join();

        // then
        assertThat(
                queue.deadLetterSequence(SEQUENCE_ID, null)
                     .orTimeout(2, TimeUnit.MILLISECONDS)
                     .join()
        ).extracting(letter -> letter.message().payload())
         .containsExactly("first", "new", "third", "unknown");
    }

    @Test
    void preservesAttributesOfMovedLetters() {
        // given
        DeadLetter<EventMessage> later = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("later"), new IllegalStateException("original failure"),
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
        ).withDiagnostics(Metadata.with("retries", "2"));
        queue.enqueue(SEQUENCE_ID, later, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> newLetter = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("new"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(1L))
        );

        // when
        DeadLetterQueueUtils.enqueueInOrder(queue, SEQUENCE_ID, newLetter, context)
                            .orTimeout(2, TimeUnit.MILLISECONDS)
                            .join();

        // then
        List<DeadLetter<? extends EventMessage>> sequence = new ArrayList<>();
        queue.deadLetterSequence(SEQUENCE_ID, null)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join().forEach(sequence::add);
        DeadLetter<? extends EventMessage> moved = sequence.get(1);
        assertThat(moved.message().identifier()).isEqualTo(later.message().identifier());
        assertThat(moved.cause()).isPresent();
        assertThat(moved.cause().isPresent()).isTrue();
        assertThat(moved.cause().get().message()).isEqualTo("original failure");
        assertThat(moved.diagnostics()).isEqualTo(Metadata.with("retries", "2"));
        assertThat(moved.enqueuedAt()).isEqualTo(later.enqueuedAt());
        assertThat(moved.position()).hasValue(9L);
    }

    @Test
    void leavesOtherSequencesUntouched() {
        // given
        DeadLetter<EventMessage> first = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("first"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
        );
        queue.enqueue(SEQUENCE_ID, first, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> other = new GenericDeadLetter<>(
                OTHER_SEQUENCE_ID, EventTestUtils.asEventMessage("other"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
        );
        queue.enqueue(OTHER_SEQUENCE_ID, other, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> newLetter = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("new"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(1L))
        );

        // when
        DeadLetterQueueUtils.enqueueInOrder(queue, SEQUENCE_ID, newLetter, context)
                            .orTimeout(2, TimeUnit.MILLISECONDS)
                            .join();

        // then
        assertThat(
                queue.deadLetterSequence(SEQUENCE_ID, null)
                     .orTimeout(2, TimeUnit.MILLISECONDS)
                     .join()
        ).extracting(letter -> letter.message().payload())
         .containsExactly("new", "first");
        assertThat(
                queue.deadLetterSequence(OTHER_SEQUENCE_ID, null)
                     .orTimeout(2, TimeUnit.MILLISECONDS)
                     .join()
        ).extracting(letter -> letter.message().payload())
         .containsExactly("other");
    }

    @Test
    void doesNotEvictAnythingWhenNoLetterIsPositionedAfterTheNewLetter() {
        // given
        SequencedDeadLetterQueue<EventMessage> spiedQueue = spy(queue);
        DeadLetter<EventMessage> first = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("first"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(1L))
        );
        queue.enqueue(SEQUENCE_ID, first, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> newLetter = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("new"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(2L))
        );

        // when
        DeadLetterQueueUtils.enqueueInOrder(spiedQueue, SEQUENCE_ID, newLetter, context)
                            .orTimeout(2, TimeUnit.MILLISECONDS)
                            .join();

        // then
        verify(spiedQueue, never()).evict(any(), any());
    }

    @Test
    void passesTheGivenContextToEveryQueueOperation() {
        // given
        SequencedDeadLetterQueue<EventMessage> spiedQueue = spy(queue);
        DeadLetter<EventMessage> later = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("later"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
        );
        queue.enqueue(SEQUENCE_ID, later, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> newLetter = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("new"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(1L))
        );

        // when
        DeadLetterQueueUtils.enqueueInOrder(spiedQueue, SEQUENCE_ID, newLetter, context)
                            .orTimeout(2, TimeUnit.MILLISECONDS)
                            .join();

        // then
        verify(spiedQueue).deadLetterSequence(SEQUENCE_ID, context);
        verify(spiedQueue).evict(any(), eq(context));
        verify(spiedQueue, times(2)).enqueue(eq(SEQUENCE_ID), any(), eq(context));
    }

    @Test
    void failsWhenEvictingAMovedLetterFails() {
        // given
        SequencedDeadLetterQueue<EventMessage> spiedQueue = spy(queue);
        DeadLetter<EventMessage> later = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("later"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
        );
        queue.enqueue(SEQUENCE_ID, later, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> newLetter = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("new"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(1L))
        );
        RuntimeException failure = new RuntimeException("evict failed");
        doReturn(CompletableFuture.failedFuture(failure)).when(spiedQueue).evict(any(), any());

        // when
        CompletableFuture<Void> result =
                DeadLetterQueueUtils.enqueueInOrder(spiedQueue, SEQUENCE_ID, newLetter, context);

        // then
        assertThatThrownBy(() -> result
                .orTimeout(2, TimeUnit.MILLISECONDS)
                .join()).isInstanceOf(CompletionException.class).hasCause(failure);
        assertThat(
                queue.deadLetterSequence(SEQUENCE_ID, null)
                     .orTimeout(2, TimeUnit.MILLISECONDS)
                     .join()
        ).extracting(letter -> letter.message().payload())
         .containsExactly("later");
    }

    @Test
    void enqueuesAtTheTailWithoutMovingAnyLetter() {
        // given
        SequencedDeadLetterQueue<EventMessage> spiedQueue = spy(queue);
        DeadLetter<EventMessage> first = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("first"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(5L))
        );
        queue.enqueue(SEQUENCE_ID, first, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> second = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("second"), null,
                Context.with(TrackingToken.RESOURCE_KEY, new GlobalSequenceTrackingToken(9L))
        );
        queue.enqueue(SEQUENCE_ID, second, context)
             .orTimeout(2, TimeUnit.MILLISECONDS)
             .join();

        DeadLetter<EventMessage> newLetter = new GenericDeadLetter<>(
                SEQUENCE_ID, EventTestUtils.asEventMessage("new"), null, Context.empty()
        );

        // when
        DeadLetterQueueUtils.enqueueInOrder(spiedQueue, SEQUENCE_ID, newLetter, context)
                            .orTimeout(2, TimeUnit.MILLISECONDS)
                            .join();

        // then
        assertThat(
                queue.deadLetterSequence(SEQUENCE_ID, null)
                     .orTimeout(2, TimeUnit.MILLISECONDS)
                     .join()
        ).extracting(letter -> letter.message().payload())
         .containsExactly("first", "second", "new");
        verify(spiedQueue, never()).evict(any(), any());
        verify(spiedQueue, never()).deadLetterSequence(any(), any());
    }
}
