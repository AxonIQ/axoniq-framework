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

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Duration;
import java.util.Objects;

/**
 * Receives queries sent by other members of the cluster over HTTP, answering them with a stream of responses.
 * <p>
 * Every member must expose this endpoint under the same path, since that is how members reach each other; the path is
 * the value of the {@code axon.springcloud.query-endpoint} property, defaulting to {@link #DEFAULT_QUERY_ENDPOINT}.
 * This controller is registered by the Spring Boot autoconfiguration.
 * <p>
 * A query is answered with a Server-Sent Events stream rather than one reply, because a query may be answered any
 * number of times. The stream releases the container thread as soon as it is returned, so a member answering a
 * long-running query does not hold one thread per query in flight.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
@RestController
@RequestMapping("${axon.springcloud.query-endpoint:" + SpringCloudQueryController.DEFAULT_QUERY_ENDPOINT + "}")
public class SpringCloudQueryController {

    /**
     * The path queries are received under when none is configured.
     */
    public static final String DEFAULT_QUERY_ENDPOINT = "/axoniq-springcloud/query";

    private final IncomingQueryInvoker invoker;
    private final Duration timeout;

    /**
     * Constructs a {@code SpringCloudQueryController} handing received queries to the given {@code invoker}.
     *
     * @param invoker the component invoking this application's local query handler
     * @param timeout how long a response stream may stay open before the container closes it. A query still being
     *                answered when it elapses is reported to the member that asked as a failed stream.
     */
    public SpringCloudQueryController(IncomingQueryInvoker invoker, Duration timeout) {
        this.invoker = Objects.requireNonNull(invoker, "The invoker must not be null.");
        this.timeout = Objects.requireNonNull(timeout, "The timeout must not be null.");
    }

    /**
     * Handles the given {@code request}, sent by another member of the cluster.
     *
     * @param request the query sent by another member
     * @return the stream carrying the responses to the query
     */
    @PostMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter receiveQuery(@RequestBody QueryDispatchRequest request) {
        SseEmitter emitter = new SseEmitter(timeout.toMillis());
        invoker.handle(request, new SseQueryResponseSink(emitter));
        return emitter;
    }
}
