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

/**
 * Abstract implementation of an MessageStream that delegates calls to a given delegate.
 *
 * @param <DM> The type of Message handled by the delegate.
 * @param <RM> The type of Message handled by this MessageStream.
 *
 * @since 5.0.0
 * @author Allard Buijze
 */
public abstract class DelegatingMessageStream<DM extends Message, RM extends Message>
        implements MessageStream<RM> {

    private final MessageStream<DM> delegate;

    /**
     * Constructs the DelegatingMessageStream with given {@code delegate} to receive calls.
     *
     * @param delegate The instance to delegate calls to.
     */
    public DelegatingMessageStream(MessageStream<DM> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void setCallback(Runnable callback) {
        delegate.setCallback(callback);
    }

    @Override
    public Optional<Throwable> error() {
        return delegate.error();
    }

    @Override
    public boolean isCompleted() {
        return delegate.isCompleted();
    }

    @Override
    public boolean hasNextAvailable() {
        return delegate.hasNextAvailable();
    }

    @Override
    public void close() {
        delegate.close();
    }

    /**
     * Returns the delegate as provided in the constructor.
     *
     * @return the delegate as provided in the constructor.
     */
    protected MessageStream<DM> delegate() {
        return delegate;
    }
}
