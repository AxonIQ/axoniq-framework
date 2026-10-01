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

package io.axoniq.framework.springcloud.query;

import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.HandlerExecutionException;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.RemoteExceptionDescription;
import org.axonframework.messaging.core.RemoteHandlingException;
import org.axonframework.messaging.core.RemoteNonTransientHandlingException;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.NoHandlerForQueryException;
import org.axonframework.messaging.queryhandling.QueryExecutionException;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.jspecify.annotations.Nullable;

import java.util.List;

import static io.axoniq.framework.springcloud.shared.WireCodec.*;

/**
 * Converts queries and their responses between the messages the framework works with and the form members send each
 * other.
 * <p>
 * A query is sent as one request and answered with a stream of events, because a query may be answered any number of
 * times. Each response is one {@link #RESPONSE_EVENT} event; a failure ending the stream is one {@link #ERROR_EVENT}
 * event. A stream that ends without an error event completed normally, however many responses it carried.
 * <p>
 * A payload travels as the text its {@link org.axonframework.messaging.core.conversion.MessageConverter} writes it
 * as, which is what the {@code PayloadConvertingQueryBusConnector} wrapped around the connector converts it to before
 * it gets here. A received payload is handed to the message exactly as it arrived, leaving the converter attached to
 * that message to read it as whatever the handler asks for.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
@Internal
final class QueryConverter {

    /**
     * The event type carrying one response to a query.
     */
    public static final String RESPONSE_EVENT = "response";

    /**
     * The event type carrying the failure that ended a query's response stream.
     */
    public static final String ERROR_EVENT = "error";

    /**
     * The event type carrying one update to a subscription query.
     * <p>
     * Told apart from a {@link #RESPONSE_EVENT} because the two mean different things to the subscriber even though
     * they carry the same shape: a response is part of the initial result, an update is a change after it. Read back
     * with {@link #convertUpdate(QueryDispatchResponse, Converter)}, so the subscriber can tell them apart too.
     */
    public static final String UPDATE_EVENT = "update";

    /**
     * The event type reporting that a subscription query is over: there will never be another update to it.
     * <p>
     * Written rather than left to the stream simply ending, because the two mean different things. A member that
     * shuts down or loses its connection ends the stream as well, and that says only that this member stopped
     * answering, which fails the subscription rather than completing it.
     */
    public static final String COMPLETE_EVENT = "complete";

    private static final boolean WRITABLE_STACK_TRACE = false;

    private QueryConverter() {
        // Utility class, not meant to be instantiated.
    }

    /**
     * Converts the given {@code query} into the request to send to another member.
     *
     * @param query the query to send
     * @return the wire representation of the given {@code query}
     * @throws ConversionException when the given {@code query}'s payload cannot be written as text
     */
    public static QueryDispatchRequest convertQueryMessage(QueryMessage query) {
        return new QueryDispatchRequest(
                query.identifier(),
                query.type().toString(),
                query.payloadAs(String.class),
                query.metadata(),
                query.priority().isPresent() ? query.priority().getAsInt() : null
        );
    }

    /**
     * Converts the given {@code request}, received from another member, into the query to handle locally.
     *
     * @param request   the request received from another member
     * @param converter the converter to attach to the resulting query for inline payload conversion, or {@code null}
     *                  when none is available.
     * @return the query the given {@code request} represents
     */
    public static QueryMessage convertRequest(QueryDispatchRequest request, @Nullable Converter converter) {
        return new GenericQueryMessage(
                new GenericMessage(
                        request.identifier(),
                        MessageType.fromString(request.type()),
                        request.payload(),
                        request.metadata()
                ),
                request.priority()
        ).withConverter(converter);
    }

    /**
     * Converts the given {@code query} into the subscription to open on another member.
     *
     * @param query            the query to subscribe with
     * @param updateBufferSize how many updates the answering member may hold for this subscriber
     * @return the wire representation of the given {@code query} as a subscription
     * @throws ConversionException when the given {@code query}'s payload cannot be written as text
     */
    public static SubscriptionQueryRequest convertSubscriptionMessage(QueryMessage query, int updateBufferSize) {
        return new SubscriptionQueryRequest(
                query.identifier(),
                query.type().toString(),
                query.payloadAs(String.class),
                query.metadata(),
                query.priority().isPresent() ? query.priority().getAsInt() : null,
                updateBufferSize
        );
    }

    /**
     * Converts the given subscription {@code request}, received from another member, into the query to handle
     * locally.
     *
     * @param request   the subscription query received from another member
     * @param converter the converter to attach to the resulting query for inline payload conversion, or {@code null}
     *                  when none is available
     * @return the query the given {@code request} represents
     */
    public static QueryMessage convertSubscriptionRequest(SubscriptionQueryRequest request,
                                                          @Nullable Converter converter) {
        return new GenericQueryMessage(
                new GenericMessage(
                        request.identifier(),
                        MessageType.fromString(request.type()),
                        request.payload(),
                        request.metadata()
                ),
                request.priority()
        ).withConverter(converter);
    }

    /**
     * Converts the given {@code response} into the event answering the query identified by {@code requestIdentifier}.
     *
     * @param response          one response to the query being answered
     * @param requestIdentifier the identifier of the query being answered
     * @return the wire representation of the given {@code response}
     * @throws ConversionException when the given {@code response}'s payload cannot be written as text
     */
    public static QueryDispatchResponse convertResponseMessage(QueryResponseMessage response, String requestIdentifier) {
        return new QueryDispatchResponse(
                response.identifier(),
                requestIdentifier,
                response.type().toString(),
                response.payloadAs(String.class),
                response.metadata()
        );
    }

    /**
     * Converts the given {@code response}, received from the member answering a query, into a response message.
     *
     * @param response  one response received from the answering member
     * @param converter the converter to attach to the response for inline payload conversion, or {@code null} when
     *                  none is available.
     * @return the response the given {@code response} represents
     */
    public static QueryResponseMessage convertResponse(QueryDispatchResponse response,
                                                       @Nullable Converter converter) {
        return new GenericQueryResponseMessage(new GenericMessage(
                response.identifier(),
                MessageType.fromString(response.type()),
                response.payload(),
                response.metadata()
        )).withConverter(converter);
    }

    /**
     * Converts the given {@code update}, received from a member holding a subscription, into an update message.
     * <p>
     * Rebuilt as a {@link SubscriptionQueryUpdateMessage} rather than as a plain response, because that type is what
     * tells an update apart from the initial result on the subscriber's side: a subscriber to the updates alone keeps
     * only the messages of that type.
     *
     * @param update    one update received from a member holding a subscription
     * @param converter the converter to attach to the update for inline payload conversion, or {@code null} when none
     *                  is available
     * @return the update the given {@code update} represents
     */
    public static SubscriptionQueryUpdateMessage convertUpdate(QueryDispatchResponse update,
                                                               @Nullable Converter converter) {
        return new GenericSubscriptionQueryUpdateMessage(new GenericMessage(
                update.identifier(),
                MessageType.fromString(update.type()),
                update.payload(),
                update.metadata()
        )).withConverter(converter);
    }

    /**
     * Converts the given {@code cause} into the event ending the response stream of the query identified by
     * {@code requestIdentifier}.
     *
     * @param cause             the failure that ended the query's response stream
     * @param requestIdentifier the identifier of the query that failed
     * @param origin            the name of this member, reported so that the dispatching member can point at where the
     *                          failure occurred.
     * @param converter         the converter to write application-specific exception details with, or {@code null}
     *                          when none is available.
     * @return the wire representation of the given {@code cause}
     */
    public static QueryDispatchFailure convertErrorResult(Throwable cause,
                                                          String requestIdentifier,
                                                          String origin,
                                                          @Nullable Converter converter) {
        byte[] details = convertedDetailsOf(cause, converter);
        Object rawDetails = HandlerExecutionException.resolveDetails(cause).orElse(null);
        return new QueryDispatchFailure(
                requestIdentifier,
                QueryErrorCode.classify(cause),
                messageOf(cause),
                descriptionsOf(cause),
                origin,
                details == null || rawDetails == null ? null : rawDetails.getClass().getName(),
                encode(details)
        );
    }

    /**
     * Reconstructs the failure the given {@code failure} reports.
     * <p>
     * The original exception type is not carried, as the member reading it need not have the class. What it does carry
     * is the distinction the dispatching member acts on: whether the failure is worth retrying.
     *
     * @param failure the failure reported by the answering member
     * @return the exception the given {@code failure} represents
     */
    public static RuntimeException convertError(QueryDispatchFailure failure) {
        String message = failure.errorMessage() == null
                ? "The member handling the query reported a failure without a message."
                : failure.errorMessage();
        String described =
                failure.errorOrigin() == null ? message : message + " [origin: " + failure.errorOrigin() + "]";
        List<String> descriptions = failure.errorDetails().isEmpty() ? List.of(message) : failure.errorDetails();
        byte[] details = decode(failure.errorDetailsPayload());

        return switch (failure.errorCode()) {
            case null -> new QueryExecutionException(
                    "The member handling the query reported a failure of an unrecognised kind. " + described,
                    new RemoteHandlingException(new RemoteExceptionDescription(descriptions)),
                    details,
                    WRITABLE_STACK_TRACE
            );
            case NO_HANDLER_FOR_QUERY -> new NoHandlerForQueryException(described);
            case QUERY_EXECUTION_ERROR -> new QueryExecutionException(
                    described,
                    new RemoteHandlingException(new RemoteExceptionDescription(descriptions)),
                    details,
                    WRITABLE_STACK_TRACE
            );
            case QUERY_EXECUTION_NON_TRANSIENT_ERROR -> new QueryExecutionException(
                    described,
                    new RemoteNonTransientHandlingException(new RemoteExceptionDescription(descriptions, true)),
                    details,
                    WRITABLE_STACK_TRACE
            );
        };
    }
}
