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

import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Objects;

/**
 * Writes a query's responses to the member that asked, as events of a Server-Sent Events stream.
 * <p>
 * Each response is written as one event, whose data the container writes with the same message converters that write
 * the body of any other response. Nothing is serialized here.
 * <p>
 * A write that fails is raised rather than swallowed: it means the member that asked is no longer reading, and the
 * caller releases the query's response stream on the strength of it.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class SseQueryResponseSink implements QueryResponseSink {

    private final SseEmitter emitter;

    /**
     * Constructs an {@code SseQueryResponseSink} writing to the given {@code emitter}.
     *
     * @param emitter the emitter carrying the response stream to the member that asked
     */
    public SseQueryResponseSink(SseEmitter emitter) {
        this.emitter = Objects.requireNonNull(emitter, "The emitter must not be null.");
    }

    @Override
    public void onUnavailable(Runnable listener) {
        Objects.requireNonNull(listener, "The listener must not be null.");
        // All three are registered because the container reports the same outcome differently depending on how the
        // stream ended, and each of them means the same thing here: stop answering. Running the listener again after
        // the query was answered is harmless, as releasing an already terminated response stream does nothing.
        emitter.onTimeout(listener);
        emitter.onCompletion(listener);
        emitter.onError(cause -> listener.run());
    }

    @Override
    public void response(QueryDispatchResponse response) {
        write(QueryConverter.RESPONSE_EVENT, response);
    }

    @Override
    public void update(QueryDispatchResponse update) {
        write(QueryConverter.UPDATE_EVENT, update);
    }

    @Override
    public void subscriptionComplete(String requestIdentifier) {
        // The identifier is written as the event's data because an event without data is not an event at all: a
        // reader following the Server-Sent Events specification discards it.
        write(QueryConverter.COMPLETE_EVENT, requestIdentifier);
        emitter.complete();
    }

    @Override
    public void leaving(String requestIdentifier) {
        write(QueryConverter.LEAVING_EVENT, requestIdentifier);
        emitter.complete();
    }

    @Override
    public void error(QueryDispatchFailure error) {
        write(QueryConverter.ERROR_EVENT, error);
        emitter.complete();
    }

    @Override
    public void complete() {
        emitter.complete();
    }

    /**
     * Writes a comment, which carries nothing but keeps an otherwise idle stream from being closed.
     * <p>
     * A subscription query may go a long time without producing an update, and an idle connection is what a load
     * balancer, proxy or NAT table reclaims. A comment is the Server-Sent Events way of saying nothing: a reader
     * following the specification discards it, and this member learns the subscriber is gone when the write fails.
     *
     * @throws ResponseStreamClosedException when the member that asked has stopped reading
     */
    public void keepAlive() {
        try {
            emitter.send(SseEmitter.event().comment("keep-alive"));
        } catch (IOException | IllegalStateException e) {
            throw new ResponseStreamClosedException(
                    "Could not keep the response stream of the member that asked alive.", e
            );
        }
    }

    private void write(String eventType, Object data) {
        try {
            // The media type is named rather than negotiated: the stream itself is text/event-stream, so there is no
            // content negotiation left to decide what the data of an event within it is written as.
            emitter.send(SseEmitter.event().name(eventType).data(data, MediaType.APPLICATION_JSON));
        } catch (IOException | IllegalStateException e) {
            // The member that asked has stopped reading, so there is nowhere to report this. Raising it lets the
            // caller release the query rather than go on producing responses nothing will receive.
            throw new ResponseStreamClosedException(
                    "Could not write to the response stream of the member that asked.", e
            );
        }
    }
}
