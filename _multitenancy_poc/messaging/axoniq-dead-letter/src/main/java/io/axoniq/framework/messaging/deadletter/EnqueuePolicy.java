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

package io.axoniq.framework.messaging.deadletter;

import org.axonframework.messaging.core.Message;

/**
 * A functional interface constructing an {@link EnqueueDecision} based on a {@link DeadLetter dead letter} and
 * {@link Throwable cause}. Should be used by components that insert dead letters into and processes dead letters from a
 * {@link SequencedDeadLetterQueue}.
 * <p>
 * Implementers of a policy can use {@link Decisions} to construct the basic types of {@code EnqueueDecision}.
 *
 * @param <M> An implementation of {@link Message} contained in the {@link DeadLetter dead letter} that will be decided
 *            on through this policy.
 * @author Steven van Beelen
 * @see Decisions
 * @since 4.6.0
 */
@FunctionalInterface
public interface EnqueuePolicy<M extends Message> {

    /**
     * Constructs a {@link EnqueueDecision} based on the given {@code letter} and {@code cause}. This operation is
     * typically invoked when handling a {@link Message} failed and a decision should be made what to do with it.
     * <p>
     * Implementers of this operation can use {@link Decisions} to construct the basic types of
     * {@code EnqueueDecision}.
     *
     * @param letter The {@link DeadLetter dead letter} implementation to make a decision on.
     * @param cause  The {@link Throwable} causing the given {@code letter} to be decided on.
     * @return The decision used to decide what to do with the given {@code letter}.
     */
    EnqueueDecision<M> decide(DeadLetter<? extends M> letter, Throwable cause);
}
