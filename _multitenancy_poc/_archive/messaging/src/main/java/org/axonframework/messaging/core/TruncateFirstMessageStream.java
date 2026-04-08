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

package org.axonframework.messaging.core;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of the {@link MessageStream} that truncates all {@link Entry entries} of the {@code delegate} stream
 * except for the first entry.
 * <p>
 * This allows users to define a {@code MessageStream} of any type and force it to a
 * {@link MessageStream.Single} stream instance.
 *
 * @param <M> The type of {@link Message} contained in the singular {@link Entry} of this stream.
 * @author Allard Buijze
 * @since 5.0.0
 */
class TruncateFirstMessageStream<M extends Message>
        extends DelegatingMessageStream<M, M>
        implements MessageStream.Single<M> {

    private final AtomicBoolean consumed = new AtomicBoolean(false);

    /**
     * Constructs the DelegatingMessageStream with given {@code delegate} to receive calls.
     *
     * @param delegate The instance to delegate calls to.
     */
    public TruncateFirstMessageStream(MessageStream<M> delegate) {
        super(delegate);
    }

    @Override
    public Optional<Entry<M>> next() {
        Optional<Entry<M>> next = delegate().next();
        if (next.isPresent() && consumed.compareAndSet(false, true)) {
            close();
            return next;
        }
        return Optional.empty();
    }

    @Override
    public void setCallback(Runnable callback) {
        super.setCallback(() -> {
            if (!consumed.get()) {
                callback.run();
            }
        });
    }

    @Override
    public Optional<Throwable> error() {
        return consumed.get() ? Optional.empty() : super.error();
    }

    @Override
    public boolean isCompleted() {
        return consumed.get() || super.isCompleted();
    }

    @Override
    public boolean hasNextAvailable() {
        return !consumed.get() && super.hasNextAvailable();
    }

    @Override
    public Optional<Entry<M>> peek() {
        if (!consumed.get()) {
            return delegate().peek();
        }
        return Optional.empty();
    }
}
