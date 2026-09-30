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

package io.axoniq.framework.springcloud.transport;

import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector.Handler;
import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.queryhandling.NoHandlerForQueryException;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Turns a {@link QueryDispatchRequest} received from another member into an invocation of this application's local
 * query handler, writing every response it produces to a {@link QueryResponseSink}.
 * <p>
 * This invoker is a component of its own, separate from the endpoint receiving the request, for the same reasons its
 * command counterpart is: the endpoint is a Spring bean built before the connector, while the
 * {@link QueryBusConnector.Handler handler} it invokes only exists once {@code DistributedQueryBus} registers one,
 * later still. The connector {@link #bind(QueryBusConnector.Handler) binds} the handler here when it receives it.
 * Keeping the work here also means the whole receiving path is testable without standing up a web stack.
 * <p>
 * A request arriving before a handler is bound is answered with a {@link QueryErrorCode#NO_HANDLER_FOR_QUERY}
 * failure, which the dispatching member sees as a transient one: this member is still starting up, and will be able
 * to answer shortly.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class IncomingQueryInvoker {

    private static final Logger logger = LoggerFactory.getLogger(IncomingQueryInvoker.class);

    private final Supplier<String> memberName;
    private final @Nullable MessageConverter converter;
    private final AtomicReference<@Nullable Handler> handler = new AtomicReference<>();

    /**
     * Constructs an {@code IncomingQueryInvoker} reporting failures as originating from the member the given
     * {@code memberName} supplies.
     * <p>
     * Taken as a supplier because a member is not named until it has registered with discovery, which happens after
     * this invoker is built. Resolving it per failure reports the name this member is actually known by, rather than
     * the provisional one it had at start-up.
     *
     * @param memberName supplies the name identifying this application in the failures it reports, used to point at
     *                   the member a failure originated on.
     * @param converter  the converter attached to received queries for inline payload conversion, and used to
     *                   serialize application-specific exception details, or {@code null} when none is available.
     */
    public IncomingQueryInvoker(Supplier<String> memberName, @Nullable MessageConverter converter) {
        this.memberName = Objects.requireNonNull(memberName, "The memberName must not be null.");
        this.converter = converter;
    }

    /**
     * Binds the {@code handler} that incoming queries are to be invoked on, replacing any previously bound one.
     *
     * @param handler the handler to invoke incoming queries on
     */
    public void bind(QueryBusConnector.Handler handler) {
        this.handler.set(Objects.requireNonNull(handler, "The handler must not be null."));
    }

    /**
     * Handles the given {@code request}, received from another member, writing its responses to the given
     * {@code sink}.
     * <p>
     * Returns as soon as the query has been handed to the local handler. Responses are written to the {@code sink} as
     * the handler produces them, which for a handler that answers asynchronously is after this method returned.
     *
     * @param request the request received from another member
     * @param sink    receives the responses to the query, and the outcome that ends them
     */
    public void handle(QueryDispatchRequest request, QueryResponseSink sink) {
        Objects.requireNonNull(request, "The request must not be null.");
        Objects.requireNonNull(sink, "The sink must not be null.");

        Handler boundHandler = handler.get();
        if (boundHandler == null) {
            logger.info("Received query [{}] before a handler was registered on this member. Reporting it as "
                                + "unhandled so the dispatching member can retry.", request.type());
            sink.error(QueryConverter.convertErrorResult(
                    new NoHandlerForQueryException(
                            "This member has not registered a query handler yet, as it is still starting up."
                    ),
                    request.identifier(), memberName.get(), converter
            ));
            return;
        }

        QueryMessage query;
        try {
            query = QueryConverter.convertRequest(request, converter);
        } catch (Exception e) {
            logger.warn("Could not read incoming query [{}] of type [{}].", request.identifier(), request.type(), e);
            // Reported as non-transient: the query never reached a handler, and sending the same bytes again cannot
            // change that, so the member that asked should not spend a retry on it.
            sink.error(QueryConverter.convertErrorResult(
                    new UnreadableQueryException("Could not read incoming query of type [" + request.type() + "].", e),
                    request.identifier(), memberName.get(), converter
            ));
            return;
        }

        MessageStream<QueryResponseMessage> responses;
        try {
            responses = boundHandler.query(query);
        } catch (Exception e) {
            logger.warn("Could not hand incoming query [{}] to the local handler.", query.type(), e);
            sink.error(QueryConverter.convertErrorResult(e, request.identifier(), memberName.get(), converter));
            return;
        }

        new ResponseWriter(request.identifier(), responses, sink).start();
    }

    /**
     * Drains a response stream into a sink, as and when the stream has something to write.
     * <p>
     * The stream reports availability on whichever thread produced it, so draining is serialised here: two threads
     * writing to one sink would interleave the responses of a single query.
     */
    private class ResponseWriter {

        private final String requestIdentifier;
        private final MessageStream<QueryResponseMessage> responses;
        private final QueryResponseSink sink;
        // Volatile rather than guarded by this writer's monitor, so that cancelling does not wait on a drain that
        // is blocked writing to a member which has already stopped reading.
        private volatile boolean terminated = false;

        private ResponseWriter(String requestIdentifier,
                               MessageStream<QueryResponseMessage> responses,
                               QueryResponseSink sink) {
            this.requestIdentifier = requestIdentifier;
            this.responses = responses;
            this.sink = sink;
        }

        private void start() {
            sink.onUnavailable(this::cancel);
            responses.setCallback(this::drain);
            drain();
        }

        /**
         * Releases the handler's response stream, because nothing is reading what it produces any more.
         * <p>
         * Deliberately not synchronised: it runs on whichever thread the sink reports on, and a drain blocked writing
         * to a member that has stopped reading is exactly the situation this has to interrupt.
         */
        private void cancel() {
            if (terminated) {
                return;
            }
            terminated = true;
            logger.debug("Nothing is reading the responses to query [{}] any more; releasing it.", requestIdentifier);
            responses.close();
        }

        private synchronized void drain() {
            if (terminated) {
                return;
            }
            try {
                while (responses.hasNextAvailable()) {
                    Optional<MessageStream.Entry<QueryResponseMessage>> next = responses.next();
                    if (next.isEmpty()) {
                        break;
                    }
                    sink.response(QueryConverter.convertResponseMessage(next.get().message(), requestIdentifier));
                }
                if (!responses.hasNextAvailable() && responses.isCompleted()) {
                    responses.error().ifPresentOrElse(this::fail, this::finish);
                }
            } catch (Exception e) {
                // Writing failed, which usually means the member that asked is gone. Reporting the failure back is
                // pointless in that case, so the stream is simply released.
                logger.debug("Could not write the responses to query [{}]; abandoning it.", requestIdentifier, e);
                terminated = true;
                responses.close();
            }
        }

        private void finish() {
            terminated = true;
            sink.complete();
        }

        private void fail(Throwable cause) {
            terminated = true;
            logger.debug("The handler of query [{}] failed.", requestIdentifier, cause);
            sink.error(QueryConverter.convertErrorResult(cause, requestIdentifier, memberName.get(), converter));
        }
    }
}
