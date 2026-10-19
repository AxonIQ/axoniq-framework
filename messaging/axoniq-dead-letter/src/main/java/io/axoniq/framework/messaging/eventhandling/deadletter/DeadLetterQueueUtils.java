/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.eventhandling.deadletter;

import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.DeadLetterQueueOverflowException;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/**
 * Utility methods for working with a {@link SequencedDeadLetterQueue}.
 *
 * @author Steven van Beelen
 * @since 5.5.0
 */
final class DeadLetterQueueUtils {

    /**
     * Enqueues the given {@code letter} in the given {@code queue} at the {@link DeadLetter#position() position} in its
     * sequence matching its position in the event stream.
     * <p>
     * Letters positioned after it are evicted and enqueued again behind it through base
     * {@link SequencedDeadLetterQueue} methods only. This touches every later letter of a given sequence, which is
     * potentially a costly operation. It is thus strongly recommended only to use this when strictly necessary.
     * <p>
     * The capacity of the sequence is verified up front, as a full queue would otherwise fail on re-enqueueing the
     * moved letters after they were evicted. The returned future completes exceptionally with a
     * {@link DeadLetterQueueOverflowException} when the {@code queue} {@link SequencedDeadLetterQueue#isFull is full}.
     * <p>
     * Falls back to a regular {@link SequencedDeadLetterQueue#enqueue(Object, DeadLetter, ProcessingContext) enqueue}
     * when the position of the letter is unknown.
     *
     * @param queue      the queue to enqueue the {@code letter} in
     * @param sequenceId the identifier of the sequence to enqueue the {@code letter} in
     * @param letter     the letter to enqueue
     * @param context    the context to enqueue the {@code letter} with
     * @return a future that completes once the {@code letter} and all moved letters are enqueued in the given
     * {@code queue}
     */
    static CompletableFuture<Void> enqueueInOrder(SequencedDeadLetterQueue<EventMessage> queue,
                                                  Object sequenceId,
                                                  DeadLetter<? extends EventMessage> letter,
                                                  ProcessingContext context) {
        OptionalLong position = letter.position();
        if (position.isEmpty()) {
            return queue.enqueue(sequenceId, letter, context);
        }

        return queue.isFull(sequenceId, context)
                    .thenCompose(
                            full -> full
                                    ? CompletableFuture.failedFuture(new DeadLetterQueueOverflowException(sequenceId))
                                    : moveAndEnqueue(queue, sequenceId, letter, context, position.getAsLong())
                    );
    }

    private static CompletableFuture<Void> moveAndEnqueue(SequencedDeadLetterQueue<EventMessage> queue,
                                                          Object sequenceId,
                                                          DeadLetter<? extends EventMessage> letter,
                                                          ProcessingContext context,
                                                          long position) {
        return queue.deadLetterSequence(sequenceId, context).thenCompose(sequence -> {
            CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
            List<DeadLetter<? extends EventMessage>> lettersToMove = getLettersToMove(sequence, position);

            // 1. Evict all the letters that should move a spot
            for (DeadLetter<? extends EventMessage> moved : lettersToMove) {
                result = result.thenCompose(v -> queue.evict(moved, context));
            }
            // 2. Enqueue the new letter
            result = result.thenCompose(v -> queue.enqueue(sequenceId, letter, context));
            // 3. Enqueue all evicted letters again, ensuring they moved a spot
            for (DeadLetter<? extends EventMessage> moved : lettersToMove) {
                result = result.thenCompose(v -> queue.enqueue(sequenceId, moved, context));
            }
            return result;
        });
    }

    private static List<DeadLetter<? extends EventMessage>> getLettersToMove(
            Iterable<DeadLetter<? extends EventMessage>> sequence,
            long position
    ) {
        boolean moving = false;
        List<DeadLetter<? extends EventMessage>> lettersToMove = new ArrayList<>();
        for (DeadLetter<? extends EventMessage> existing : sequence) {
            OptionalLong existingPosition = existing.position();
            moving = moving || (existingPosition.isPresent() && existingPosition.getAsLong() > position);
            if (moving) {
                lettersToMove.add(existing);
            }
        }
        return lettersToMove;
    }

    private DeadLetterQueueUtils() {
        // Utility class
    }
}
