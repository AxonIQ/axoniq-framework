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
        T next = stream.nextIfAvailable();

        if (next != null) {
            if (isError(next)) {
                return FetchResult.error(createAxonException(next));
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
}
