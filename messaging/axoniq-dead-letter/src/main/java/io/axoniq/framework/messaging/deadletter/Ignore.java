/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
