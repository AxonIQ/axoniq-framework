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

package io.axoniq.framework.axonserver.connector.query;

import io.axoniq.axonserver.connector.ResultStream;
import org.axonframework.common.AxonException;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.AbstractMessageStream;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;

import static java.util.Objects.requireNonNull;

/**
 * An abstract implementation of the {@link MessageStream} interface that wraps a {@link ResultStream}. This class
 * provides functionality for transforming the data in the {@link ResultStream} into {@link QueryResponseMessage}s,
 * handling any encountered errors, and managing stream lifecycle events.
 *
 * @param <T> The type of the objects in the underlying {@link ResultStream} to be transformed into
 *            {@link QueryResponseMessage}s.
 * @author Allard Buijze
 * @author Jan Gallinkski
 * @author John Hendrikx
 * @since 5.0.0
 */
@Internal
public abstract class AbstractQueryResponseMessageStream<T> extends AbstractMessageStream<QueryResponseMessage> {

    private final ResultStream<T> stream;

    /**
     * Constructs an instance of the AbstractQueryResponseMessageStream class with the provided result stream.
     *
     * @param stream The {@link ResultStream} instance from which query response data will be fetched. Must not be
     *               null.
     */
    protected AbstractQueryResponseMessageStream(ResultStream<T> stream) {
        this.stream = requireNonNull(stream, "The query result stream cannot be null.");

        stream.onAvailable(this::signalProgress);
    }

    @Override
    protected FetchResult<Entry<QueryResponseMessage>> fetchNext() {
        T next;
        while ((next = stream.nextIfAvailable()) != null) {
            if (isError(next)) {
                return FetchResult.error(createAxonException(next));
            }

            if (isEmptyResult(next)) {
                // No payload was produced for this entry (e.g. a null/Optional.empty() query result) — skip it
                // instead of surfacing it as an element, so the resulting MessageStream stays empty.
                continue;
            }

            return FetchResult.of(new SimpleEntry<>(buildResponseMessage(next), Context.empty()));
        }

        if (stream.getError().isPresent()) {
            return FetchResult.error(stream.getError().orElseThrow());
        }

        return stream.isClosed() ? FetchResult.completed() : FetchResult.notReady();
    }

    @Override
    protected void onCompleted() {
        if (!stream.isClosed()) {
            stream.close();
        }
    }

    abstract QueryResponseMessage buildResponseMessage(T t);

    abstract AxonException createAxonException(T t);

    protected abstract boolean isError(T t);

    /**
     * Whether the given {@code t} represents an entry that carries no payload and should be skipped rather than
     * surfaced as a {@link MessageStream} entry.
     *
     * @param t the entry to check
     * @return {@code true} if {@code t} carries no payload, {@code false} otherwise
     * @deprecated as this method purely exists for interoperability between Axon Framework 4 and Axon Framework 5
     */
    @Deprecated(forRemoval = true, since = "5.2.1")
    protected boolean isEmptyResult(T t) {
        return false;
    }
}
