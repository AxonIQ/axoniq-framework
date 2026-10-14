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
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.WrappedToken;

import java.util.List;
import java.util.OptionalLong;

/**
 * The decision on how the {@link DeadLetteringEventHandlingComponent} handles an {@link EventMessage} that is replayed
 * while its sequence still holds {@link DeadLetter dead letters}.
 * <p>
 * During a regular run, an event whose sequence holds dead letters is always enqueued behind them to preserve ordering.
 * Doing this during a replay will cause problems, however. The events positioned <em>before</em> the first dead letter
 * of the sequence may be needed to rebuild the state that dead letter depends on, and the events that are
 * <em>already</em> dead-lettered should not be enqueued a second time.
 * <p>
 * This enumeration names the possible outcomes, and {@link #decide} picks one by comparing the position of the replayed
 * event with the positions of the dead letters in its sequence.
 *
 * @author Steven van Beelen
 * @since 5.5.0
 */
enum ReplayHandleDecision {

    /**
     * The replayed event is not part of the sequence, and its position or that of the first dead letter is unknown, so
     * they cannot be compared.
     * <p>
     * The event is handled as it would be outside a replay. Thus, enqueued behind the existing dead letters of its
     * sequence.
     */
    REGULAR,

    /**
     * The replayed event is not part of the sequence, and is positioned before the first dead letter of its sequence.
     * It should thus be handled.
     * <p>
     * Should handling fail, the resulting dead letter is enqueued at its position in the sequence, instead of at the
     * end.
     */
    HANDLE,

    /**
     * The replayed event is already part of the sequence, hence should be skipped, regardless of its position.
     */
    SKIP,

    /**
     * The replayed event is not part of the sequence, and is positioned at or after the first dead letter.
     * <p>
     * The event is enqueued without being handled, at the position in the sequence matching its place in the event
     * stream. Enqueueing it behind the existing dead letters would break the ordering, as the sequence could already
     * hold dead letters positioned after it. Handling it is not an option either, as the first dead letter blocks the
     * sequence.
     * <p>
     * This is expected to happen rarely, in two situations:
     * <ol>
     *     <li>A replayed event that was {@link #HANDLE handled} failed, and was enqueued as the new first dead letter
     *     of the sequence. The replayed events that follow it, and that are still positioned before the dead letters
     *     that were already enqueued, are not part of the sequence yet. Without this decision they would be skipped
     *     and lost, or enqueued behind dead letters positioned after them.</li>
     *     <li>A dead letter was evicted from the sequence without being the first one, for example by an operator,
     *     so the event it represented is no longer part of the sequence. Replaying that event again restores it.</li>
     * </ol>
     */
    ENQUEUE_IN_ORDER;

    /**
     * Decides how to handle the given replayed {@code event}, given the {@code letters} that are currently enqueued for
     * its sequence.
     * <p>
     * An event that is already part of the sequence is skipped first, matched on its
     * {@link EventMessage#identifier() identifier}. For the remaining events, the position is taken from the
     * {@link TrackingToken} in the given {@code context}, and compared with the position of the first of the
     * {@code letters}. The outcome is:
     * <ul>
     *     <li>{@link #SKIP} when the identifier matches that of an enqueued letter,</li>
     *     <li>{@link #REGULAR} when either position is unknown,</li>
     *     <li>{@link #HANDLE} when the event is positioned before the first letter,</li>
     *     <li>{@link #ENQUEUE_IN_ORDER} when the event is positioned at or after the first letter.</li>
     * </ul>
     *
     * @param event   the replayed event to decide on
     * @param context the context of the replayed {@code event}, holding the {@link TrackingToken} describing its
     *                position
     * @param letters the dead letters currently enqueued for the sequence of the {@code event}, in sequence order, with
     *                the first letter being the head of the sequence. Must not be empty
     * @return the decision on how to handle the replayed {@code event}
     */
    static ReplayHandleDecision decide(EventMessage event,
                                       ProcessingContext context,
                                       List<DeadLetter<? extends EventMessage>> letters) {
        boolean alreadyPresent = letters.stream().anyMatch(l -> l.message().identifier().equals(event.identifier()));
        if (alreadyPresent) {
            return SKIP;
        }

        OptionalLong eventPosition = TrackingToken.fromContext(context)
                                                  .map(token -> WrappedToken.unwrapLowerBound(token).position())
                                                  .orElse(OptionalLong.empty());
        OptionalLong headPosition = letters.getFirst().position();
        if (eventPosition.isEmpty() || headPosition.isEmpty()) {
            return REGULAR;
        }
        return eventPosition.getAsLong() < headPosition.getAsLong() ? HANDLE : ENQUEUE_IN_ORDER;
    }
}
