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

        return queue.deadLetterSequence(sequenceId, context).thenCompose(sequence -> {
            CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
            List<DeadLetter<? extends EventMessage>> lettersToMove = getLettersToMove(sequence, position.getAsLong());

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
