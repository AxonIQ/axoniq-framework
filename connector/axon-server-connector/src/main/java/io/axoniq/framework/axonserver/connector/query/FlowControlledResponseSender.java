/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.query;

import io.axoniq.axonserver.connector.FlowControl;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.framework.messaging.distributed.CallbackHandOff;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.axoniq.framework.axonserver.connector.query.QueryConverter.*;

/**
 * Implementation of the {@link FlowControl} interface that sends {@link QueryResponse}s provided by a
 * {@link MessageStream} to a downstream {@link ReplyChannel}.
 *
 * @author Allard Buijze
 * @since 5.0.0
 */
@Internal
class FlowControlledResponseSender implements FlowControl {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final String clientId;
    private final String queryIdentifier;
    private final MessageStream<QueryResponseMessage> upstream;
    private final ReplyChannel<QueryResponse> downstream;
    private final @Nullable Converter converter;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong requests = new AtomicLong();
    private final AtomicBoolean sendingGate = new AtomicBoolean(false);
    private final AtomicBoolean anySent = new AtomicBoolean(false);

    /**
     * Constructs a {@code FlowControlledResponseSender} that sends {@code upstream}'s messages to {@code downstream}.
     *
     * @param clientId        the identifier of this application, used as the location reported in error responses
     * @param queryIdentifier the identifier correlating responses to the query that led to this response sender
     * @param upstream        the {@link MessageStream} providing the {@link QueryResponseMessage}s to send
     * @param downstream      the {@link ReplyChannel} to send converted {@link QueryResponse}s to
     * @param converter       the {@link Converter} to use for serializing application-specific exception details onto
     *                        an error response, or {@code null} if no such conversion is available
     */
    public FlowControlledResponseSender(String clientId,
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
        if (requested <= 0) {
            // ignore those
            return;
        }
        if (requests.getAndUpdate(current ->
                                          current > Long.MAX_VALUE - requested ? Long.MAX_VALUE :
                                                  current + requested) == 0) {
            // The stream calls back on the thread of whoever produced a response, which may not read from it, or block
            upstream.setCallback(CallbackHandOff.handOff(this::responseSendingLoop));
        }
        responseSendingLoop();
    }

    private void responseSendingLoop() {
        // this is to make sure that we check the status again if the gate was flipped to false
        // there may have been messages that were sent after the last check
        while (!sendingGate.get()) {
            boolean hasNext = upstream.hasNextAvailable();
            boolean shouldSend = (requests.get() > 0 && hasNext)
                    || (!hasNext && upstream.isCompleted() && !closed.get());
            if (!shouldSend) {
                return;
            }
            sendResponses();
        }
    }

    private void sendResponses() {
        if (sendingGate.getAndSet(true)) {
            // the sending task is already being executed
            return;
        }
        try {
            while (requests.get() > 0 && upstream.hasNextAvailable()) {
                var next = upstream.next();
                if (next.isPresent()) {
                    requests.decrementAndGet();
                }
                next.ifPresent(i -> {
                    anySent.set(true);
                    downstream.send(convertQueryResponseMessage(queryIdentifier, i.message()));
                });
            }
            if (!upstream.hasNextAvailable() && upstream.isCompleted()) {
                closed.set(true);
                upstream.error().ifPresentOrElse(
                        error -> downstream.sendLast(buildErrorResponse(clientId, queryIdentifier, error, converter)),
                        () -> {
                            if (anySent.get()) {
                                downstream.complete();
                            } else {
                                // A direct query must yield at least one response on the wire, to
                                // remain compatible with the Axon Framework 4 wire protocol.
                                downstream.sendLast(emptyQueryResponse(queryIdentifier));
                            }
                        }
                );
            }
        } catch (RuntimeException e) {
            fail(e);
        } finally {
            sendingGate.set(false);
        }
    }

    /*
     * Ends the query with the given failure, as nothing reads the stream after this, and nobody else would tell
     * the client: it would wait for the query to time out.
     */
    private void fail(RuntimeException failure) {
        if (closed.getAndSet(true)) {
            // The terminal response was already being sent when this failed; there is nothing left to tell the client
            logger.warn("Failed to complete the response to query [{}].", queryIdentifier, failure);
            return;
        }

        upstream.close();

        try {
            downstream.sendLast(buildErrorResponse(clientId, queryIdentifier, failure, converter));
        } catch (RuntimeException e) {
            // Most likely the failure's details could not be converted: retry without them
            try {
                downstream.sendLast(buildErrorResponse(clientId, queryIdentifier, failure, null));
            } catch (RuntimeException ex) {
                ex.addSuppressed(failure);
                logger.warn("Failed to report a failure to the client for query [{}].", queryIdentifier, ex);
            }
        }
    }

    @Override
    public void cancel() {
        upstream.close();
    }
}
