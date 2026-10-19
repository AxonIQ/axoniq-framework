/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.deadletter;

import org.axonframework.messaging.core.Message;

import java.util.Optional;

/**
 * An {@link EnqueueDecision} stating a {@link DeadLetter dead letter} should not be enqueued.
 *
 * @param <M> An implementation of {@link Message} contained in the {@link DeadLetter dead letter} that is decided on.
 * @author Steven van Beelen
 * @since 4.6.0
 */
public class DoNotEnqueue<M extends Message> implements EnqueueDecision<M> {

    @Override
    public boolean shouldEnqueue() {
        return false;
    }

    @Override
    public Optional<Throwable> enqueueCause() {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "DoNotEnqueue{}";
    }
}
