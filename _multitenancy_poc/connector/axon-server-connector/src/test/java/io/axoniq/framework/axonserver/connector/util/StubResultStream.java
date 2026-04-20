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

package io.axoniq.framework.axonserver.connector.util;

import io.axoniq.axonserver.connector.ResultStream;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;

public class StubResultStream<T> implements ResultStream<T> {

    private final Iterator<T> responses;
    private final Throwable error;
    private T peeked;
    private volatile boolean closed;
    private final int totalNumberOfElements;

    public StubResultStream(Throwable error) {
        this.error = error;
        this.closed = true;
        this.responses = Collections.emptyIterator();
        this.totalNumberOfElements = 1;
    }

    @SafeVarargs
    public StubResultStream(T... responses) {
        this.error = null;
        List<T> queryResponses = asList(responses);
        this.responses = queryResponses.iterator();
        this.totalNumberOfElements = queryResponses.size();
        this.closed = totalNumberOfElements == 0;
    }

    @Override
    public T peek() {
        if (peeked == null && responses.hasNext()) {
            peeked = responses.next();
        }
        return peeked;
    }

    @Override
    public T nextIfAvailable() {
        if (peeked != null) {
            T result = peeked;
            peeked = null;
            closeIfThereAreNoMoreElements();
            return result;
        }
        if (responses.hasNext()) {
            T next = responses.next();
            closeIfThereAreNoMoreElements();
            return next;
        } else {
            return null;
        }
    }

    private void closeIfThereAreNoMoreElements() {
        if (!responses.hasNext() && !isClosed()) {
            close();
        }
    }

    @Override
    public T nextIfAvailable(long timeout, TimeUnit unit) {
        return nextIfAvailable();
    }

    @Override
    public T next() {
        return nextIfAvailable();
    }

    @Override
    public void onAvailable(Runnable r) {
        if (peeked != null || responses.hasNext() || isClosed()) {
            IntStream.rangeClosed(0, totalNumberOfElements)
                     .forEach(i -> r.run());
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public Optional<Throwable> getError() {
        return Optional.ofNullable(error);
    }
}
