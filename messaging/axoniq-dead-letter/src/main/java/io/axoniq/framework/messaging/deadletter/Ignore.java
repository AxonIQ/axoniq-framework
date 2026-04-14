/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.framework.messaging.deadletter;

import org.axonframework.messaging.core.Message;

import java.util.Optional;

/**
 * An {@link EnqueueDecision} stating a {@link DeadLetter dead letter} should be ignored.
 * <p>
 * This means the decision can be ignored entirely. As such the component enqueueing a letter will decide what to do
 * with it. In most scenarios this result in enqueueing the given {@code letter}, or keeping it in the queue.
 * <p>
 * Do not that an {@code EnqueueDecision} only impacts how, and if, to enqueue a dead letter, and nothing more.
 *
 * @param <M> An implementation of {@link Message} contained in the {@link DeadLetter dead letter} that's been made a
 *            decision on.
 * @author Steven van Beelen
 * @since 4.6.0
 */
public class Ignore<M extends Message> implements EnqueueDecision<M> {

    @Override
    public boolean shouldEnqueue() {
        return true;
    }

    @Override
    public Optional<Throwable> enqueueCause() {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "Ignore{}";
    }
}
