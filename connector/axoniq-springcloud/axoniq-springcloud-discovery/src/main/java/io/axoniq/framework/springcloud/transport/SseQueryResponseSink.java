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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Objects;

/**
 * Writes a query's responses to the member that asked, as events of a Server-Sent Events stream.
 * <p>
 * Each response is written as one event whose data is the JSON the reading member parses. The JSON is written here
 * rather than left to content negotiation, so that both ends of the stream agree on the encoding by construction.
 * <p>
 * A write that fails is raised rather than swallowed: it means the member that asked is no longer reading, and the
 * caller releases the query's response stream on the strength of it.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class SseQueryResponseSink implements QueryResponseSink {

    private final SseEmitter emitter;
    private final ObjectMapper objectMapper;

    /**
     * Constructs an {@code SseQueryResponseSink} writing to the given {@code emitter}.
     *
     * @param emitter      the emitter carrying the response stream to the member that asked
     * @param objectMapper the mapper writing each event's data
     */
    public SseQueryResponseSink(SseEmitter emitter, ObjectMapper objectMapper) {
        this.emitter = Objects.requireNonNull(emitter, "The emitter must not be null.");
        this.objectMapper = Objects.requireNonNull(objectMapper, "The objectMapper must not be null.");
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
    public void response(QueryResponseEvent response) {
        write(QueryConverter.RESPONSE_EVENT, response);
    }

    @Override
    public void error(QueryErrorEvent error) {
        write(QueryConverter.ERROR_EVENT, error);
        emitter.complete();
    }

    @Override
    public void complete() {
        emitter.complete();
    }

    private void write(String eventType, Object data) {
        try {
            emitter.send(SseEmitter.event().name(eventType).data(objectMapper.writeValueAsString(data)));
        } catch (IOException | IllegalStateException e) {
            // The member that asked has stopped reading, so there is nowhere to report this. Raising it lets the
            // caller release the query rather than go on producing responses nothing will receive.
            throw new ResponseStreamClosedException(
                    "Could not write to the response stream of the member that asked.", e
            );
        }
    }
}
