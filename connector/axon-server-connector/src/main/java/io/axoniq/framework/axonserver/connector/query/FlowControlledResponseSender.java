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
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.framework.axonserver.connector.shared.ErrorCode;
import io.axoniq.framework.axonserver.connector.shared.ExceptionConverter;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of the {@link FlowControl} interface that sends {@link QueryResponse}s provided by a
 * {@link MessageStream} to a downstream {@link ReplyChannel}.
 *
 * @author Allard Buijze
 * @since 5.0.0
 */
@Internal
class FlowControlledResponseSender implements FlowControl {

    private final String clientId;
    private final String queryIdentifier;
    private final MessageStream<QueryResponseMessage> upstream;
    private final ReplyChannel<QueryResponse> downstream;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong requests = new AtomicLong();
    private final AtomicBoolean sendingGate = new AtomicBoolean(false);
    private final AtomicBoolean anySent = new AtomicBoolean(false);

    public FlowControlledResponseSender(String clientId,
                                        String queryIdentifier,
                                        MessageStream<QueryResponseMessage> upstream,
                                        ReplyChannel<QueryResponse> downstream) {
        this.clientId = clientId;
        this.queryIdentifier = queryIdentifier;
        this.upstream = upstream;
        this.downstream = downstream;
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
            upstream.setCallback(this::responseSendingLoop);
        }
        responseSendingLoop();
    }

    private void responseSendingLoop() {
        // this is to make sure that we check the status again if the gate was flipped to false
        // there may have been messages that were sent after the last check
        while (!sendingGate.get() && ((requests.get() > 0 && upstream.hasNextAvailable()) || (!upstream.hasNextAvailable() && upstream.isCompleted()
                && !closed.get()))) {
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
                    downstream.send(QueryConverter.convertQueryResponseMessage(queryIdentifier, i.message()));
                });
            }
            if (!upstream.hasNextAvailable() && upstream.isCompleted()) {
                closed.set(true);
                upstream.error().ifPresentOrElse(
                        error -> {
                            ErrorCode errorCode = ErrorCode.getQueryExecutionErrorCode(error);
                            ErrorMessage ex = ExceptionConverter.convertToErrorMessage(clientId, errorCode, error);
                            QueryResponse errorResponse =
                                    QueryResponse.newBuilder()
                                                 .setErrorCode(errorCode.errorCode())
                                                 .setErrorMessage(ex)
                                                 .setRequestIdentifier(queryIdentifier)
                                                 .build();
                            downstream.sendLast(errorResponse);
                        },
                        () -> {
                            if (anySent.get()) {
                                downstream.complete();
                            } else {
                                // A direct query must yield at least one response on the wire, to
                                // remain compatible with the Axon Framework 4 wire protocol.
                                downstream.sendLast(QueryConverter.emptyQueryResponse(queryIdentifier));
                            }
                        }
                );
            }
        } finally {
            sendingGate.set(false);
        }
    }

    @Override
    public void cancel() {
        upstream.close();
    }
}
