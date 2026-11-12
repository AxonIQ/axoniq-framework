/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import org.axonframework.messaging.core.DelegatingMessageStream;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps a stream so that its consumer is forced to continue on another thread after every single entry.
 * <p>
 * Whenever the consumer has read an entry, this stream reports that nothing is available until it has signalled the
 * consumer's callback from a thread of the given executor. A consumer such as {@link MessageStream#reduce} then
 * resumes there, and does all of its further reading, which includes whatever the wrapped stream does to fetch more
 * entries, on that thread. This simulates a stream that temporarily ran dry and later resumed on some other thread.
 *
 * @param <M> the type of message in the stream
 * @author John Hendrikx
 */
final class HoppingMessageStream<M extends Message> extends DelegatingMessageStream<M, M> {

    private final Executor hopExecutor;
    private final AtomicBoolean released = new AtomicBoolean();
    private final Set<String> hopThreadNames = ConcurrentHashMap.newKeySet();

    private volatile Runnable callback = () -> {
    };

    /**
     * Constructs a new instance.
     *
     * @param delegate    the stream to wrap
     * @param hopExecutor the executor whose threads signal the consumer
     */
    HoppingMessageStream(MessageStream<M> delegate, Executor hopExecutor) {
        super(delegate);

        this.hopExecutor = hopExecutor;
    }

    /**
     * Returns the names of all threads that have signalled the consumer so far.
     *
     * @return the thread names, never {@code null}
     */
    Set<String> hopThreadNames() {
        return hopThreadNames;
    }

    @Override
    public void setCallback(Runnable callback) {
        this.callback = callback;

        delegate().setCallback(this::hop);

        hop();
    }

    @Override
    public boolean hasNextAvailable() {
        return released.get() && delegate().hasNextAvailable();
    }

    @Override
    public boolean isCompleted() {
        return released.get() && delegate().isCompleted();
    }

    @Override
    public Optional<Entry<M>> peek() {
        return released.get() ? delegate().peek() : Optional.empty();
    }

    @Override
    public Optional<Entry<M>> next() {
        if (!released.get()) {
            return Optional.empty();
        }

        Optional<Entry<M>> entry = delegate().next();

        if (entry.isPresent()) {
            released.set(false);

            hop();
        }

        return entry;
    }

    private void hop() {
        hopExecutor.execute(() -> {
            hopThreadNames.add(Thread.currentThread().getName());
            released.set(true);
            callback.run();
        });
    }
}
