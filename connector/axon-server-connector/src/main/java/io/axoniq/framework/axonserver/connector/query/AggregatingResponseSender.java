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

import io.axoniq.axonserver.connector.FlowControl;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.axoniq.framework.axonserver.connector.query.QueryConverter.*;

/**
 * Implementation of the {@link FlowControl} interface that drains an entire {@link MessageStream} of
 * {@link QueryResponseMessage QueryResponseMessages} and sends the result as a single {@link QueryResponse}.
 * <p>
 * Used instead of {@link FlowControlledResponseSender} when the querying client did not declare support for receiving
 * multiple responses to a single query (e.g. an Axon Framework 4 client). Such a client always expects exactly one
 * {@code QueryResponse} on the wire for a query, regardless of how many {@code QueryResponseMessages} the local handler
 * produced. A {@code @QueryHandler} returning a collection is spread by the framework into one
 * {@code QueryResponseMessage} per element (to support {@code queryMany}), so this class recombines those elements into
 * a single {@code QueryResponseMessage} carrying the full collection as its payload before sending it.
 *
 * @author Steven van Beelen
 * @since 5.3.1
 * @deprecated as this method purely exists for interoperability between Axon Framework 4 and Axon Framework 5
 */
@Deprecated(forRemoval = true, since = "5.3.1")
@Internal
class AggregatingResponseSender implements FlowControl {

    private final String clientId;
    private final String queryIdentifier;
    private final MessageStream<QueryResponseMessage> upstream;
    private final ReplyChannel<QueryResponse> downstream;
    private final @Nullable Converter converter;
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * Constructs an {@code AggregatingResponseSender} that sends {@code upstream}'s combined messages to
     * {@code downstream}.
     *
     * @param clientId        the identifier of this application, used as the location reported in error responses
     * @param queryIdentifier the identifier correlating the response to the query that led to this response sender
     * @param upstream        the {@link MessageStream} providing the {@link QueryResponseMessage}s to combine and send
     * @param downstream      the {@link ReplyChannel} to send the converted {@link QueryResponse} to
     * @param converter       the {@link Converter} to use for decoding individual payloads when combining them, and for
     *                        serializing application-specific exception details onto an error response, or {@code null}
     *                        if no such conversion is available
     */
    public AggregatingResponseSender(String clientId,
                                     String queryIdentifier,
                                     MessageStream<QueryResponseMessage> upstream,
                                     ReplyChannel<QueryResponse> downstream,
                                     @Nullable Converter converter) {
        this.clientId = clientId;
        this.queryIdentifier = queryIdentifier;
        this.upstream = upstream;
        this.downstream = downstream;
        this.converter = converter;
    }

    @Override
    public void request(long requested) {
        if (requested <= 0 || !started.compareAndSet(false, true)) {
            return;
        }
        upstream.collect(ArrayList<QueryResponseMessage>::new, List::add)
                .whenComplete((messages, error) -> {
                    if (error != null) {
                        downstream.sendLast(buildErrorResponse(clientId, queryIdentifier, error, converter));
                    } else if (messages.isEmpty()) {
                        // A direct query must yield at least one response on the wire, to
                        // remain compatible with the Axon Framework 4 wire protocol.
                        downstream.sendLast(emptyQueryResponse(queryIdentifier));
                    } else if (messages.size() == 1) {
                        downstream.sendLast(convertQueryResponseMessage(queryIdentifier, messages.getFirst()));
                    } else {
                        try {
                            downstream.sendLast(convertQueryResponseMessage(queryIdentifier, combine(messages)));
                        } catch (Exception combineError) {
                            downstream.sendLast(buildErrorResponse(clientId, queryIdentifier, combineError, converter));
                        }
                    }
                });
    }

    /**
     * Combines the payloads of the given {@code messages} into a single {@link QueryResponseMessage} carrying an array
     * of all payloads.
     * <p>
     * Each payload is decoded into the concrete type named by its {@link QueryResponseMessage#type()} (resolved via
     * {@link Class#forName(String)}, matching how message types are named by default in the first place), since a
     * generic decode target such as {@code Object.class} would trivially match the still-serialized {@code byte[]}
     * payload without ever invoking the {@code converter}.
     * <p>
     * The combined payload is a Java array of that element type, rather than a {@link List}: an array's runtime class
     * retains its component type (e.g. {@code CustomerDto[]}), whereas a {@code List}'s runtime class
     * ({@code ArrayList}) does not carry its generic type. A client without streaming support like Axon Framework 4
     * resolves the class to deserialize into directly from the combined message's declared type, so only the array's
     * type lets it deserialize into properly-typed elements instead of raw maps.
     *
     * @param messages the {@link QueryResponseMessage}s to combine, must contain at least two entries
     * @return a single {@link QueryResponseMessage} carrying all of {@code messages}' payloads as an array
     * @throws IllegalStateException if the element type named by the first message's
     *                               {@link QueryResponseMessage#type()} cannot be resolved to a {@link Class}
     */
    private QueryResponseMessage combine(List<QueryResponseMessage> messages) {
        QueryResponseMessage first = messages.getFirst();
        // We strictly anticipate the MessageType#name to be the Class, which is an Axon Framework 4 style.
        // This assumption is fair, as for AF4 to AF5 compatibility the user **must** follow this message typing style.
        Class<?> elementType = resolveElementType(first.type().name());
        Object combinedPayload = Array.newInstance(elementType, messages.size());
        for (int i = 0; i < messages.size(); i++) {
            Array.set(combinedPayload, i, messages.get(i).payloadAs(elementType, converter));
        }
        // Class#getName() (not the "friendly" MessageType(Class) derivation) is used here, since an array's
        // binary name (e.g. "[Lorg.example.CustomerDto;") is what Class.forName can resolve back on the
        // receiving end — its "friendly" name (e.g. "org.example.CustomerDto[]") is not.
        MessageType arrayType = new MessageType(new QualifiedName(combinedPayload.getClass().getName()));
        return new GenericQueryResponseMessage(arrayType, combinedPayload, first.metadata())
                .withConverter(converter);
    }

    private static Class<?> resolveElementType(String typeName) {
        try {
            return Class.forName(typeName);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "Cannot combine multiple query results into a single response for a client that does not "
                            + "support receiving multiple responses: the element type [" + typeName + "] could not "
                            + "be resolved to a class.",
                    e
            );
        }
    }

    @Override
    public void cancel() {
        upstream.close();
    }
}
