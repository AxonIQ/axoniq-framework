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

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Receives queries sent by other members of the cluster over HTTP, answering them with a stream of responses.
 * <p>
 * Every member must expose this endpoint under the same path, since that is how members reach each other; the path is
 * the value of the {@code axon.springcloud.query-endpoint} property, defaulting to {@link #DEFAULT_QUERY_ENDPOINT}.
 * Subscription queries are received one level below it, under {@link #SUBSCRIPTION_PATH}. This controller is
 * registered by the Spring Boot autoconfiguration.
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

    /**
     * The path, relative to the query endpoint, subscription queries are received under.
     */
    public static final String SUBSCRIPTION_PATH = "/subscription";

    /**
     * How often an idle subscription is written to when no other interval is configured.
     * <p>
     * Comfortably inside the sixty seconds a load balancer, proxy or NAT table commonly gives an idle connection, so
     * that several keep-alives pass before any of them would reclaim it.
     */
    public static final Duration DEFAULT_KEEP_ALIVE_INTERVAL = Duration.ofSeconds(20);

    /**
     * The emitter timeout given to a subscription: none. A subscription ends when the subscriber releases it or this
     * member fails it, not when a clock says so.
     */
    private static final long NO_TIMEOUT = 0L;

    private static final Logger logger = LoggerFactory.getLogger(SpringCloudQueryController.class);

    private final IncomingQueryGateway gateway;
    private final Duration timeout;
    private final Duration keepAliveInterval;
    private final ScheduledExecutorService scheduler;

    /**
     * Constructs a {@code SpringCloudQueryController} handing received queries to the given {@code gateway}.
     *
     * @param gateway           the gateway invoking this application's local query handler
     * @param timeout           how long a response stream may stay open before the container closes it. A query still
     *                          being answered when it elapses is reported to the member that asked as a failed
     *                          stream. Does not apply to a subscription query, which lasts as long as the subscriber
     *                          wants it to.
     * @param keepAliveInterval how often an idle subscription is written to, so that neither the subscribing member
     *                          nor the intermediaries between the two mistake a quiet subscription for a dead one.
     *                          Must be comfortably below the window subscribing members give a subscription to say
     *                          something.
     * @param scheduler         runs the keep-alive of each open subscription
     */
    public SpringCloudQueryController(IncomingQueryGateway gateway,
                                      Duration timeout,
                                      Duration keepAliveInterval,
                                      ScheduledExecutorService scheduler) {
        this.gateway = Objects.requireNonNull(gateway, "The gateway must not be null.");
        this.timeout = Objects.requireNonNull(timeout, "The timeout must not be null.");
        Objects.requireNonNull(keepAliveInterval, "The keepAliveInterval must not be null.");
        if (keepAliveInterval.isNegative() || keepAliveInterval.isZero()) {
            throw new IllegalArgumentException(
                    "The keep-alive interval must be positive, but was [" + keepAliveInterval + "]."
            );
        }
        this.keepAliveInterval = keepAliveInterval;
        this.scheduler = Objects.requireNonNull(scheduler, "The scheduler must not be null.");
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
        gateway.handle(request, new SseQueryResponseSink(emitter));
        return emitter;
    }

    /**
     * Handles the given subscription {@code request}, sent by another member of the cluster.
     * <p>
     * The stream carrying it has no timeout: a subscription lasts until the subscriber releases it or this member
     * fails it, and a member that is simply not emitting updates is behaving correctly. What stands in for a timeout
     * is the keep-alive written while it is idle, which stops as soon as the subscriber stops reading.
     *
     * @param request the subscription query sent by another member
     * @return the stream carrying the initial result and the updates that follow it
     */
    @PostMapping(path = SUBSCRIPTION_PATH, produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter receiveSubscriptionQuery(@RequestBody SubscriptionQueryRequest request) {
        SseEmitter emitter = new SseEmitter(NO_TIMEOUT);
        SseQueryResponseSink sink = new SseQueryResponseSink(emitter);
        // Registered before anything is written, so that the response reaching the subscribing member means the
        // subscription is in place. That member asks for the initial result on the strength of it.
        gateway.handleSubscription(request, sink);
        new KeepAlive(sink, request.identifier()).start();
        return emitter;
    }

    /**
     * Writes to a subscription while it has nothing of its own to say, and stops once it can no longer be written to.
     */
    private final class KeepAlive implements Runnable {

        private final SseQueryResponseSink sink;
        private final String requestIdentifier;

        private volatile @Nullable ScheduledFuture<?> beat;

        private KeepAlive(SseQueryResponseSink sink, String requestIdentifier) {
            this.sink = sink;
            this.requestIdentifier = requestIdentifier;
        }

        private void start() {
            // Beaten at once rather than only after the first interval. A stream nothing has been written to does
            // not reach the subscribing member at all, and until it does that member cannot know the subscription
            // is registered, nor ask for the initial result without risking an update emitted in between.
            run();
            long intervalMillis = keepAliveInterval.toMillis();
            beat = scheduler.scheduleWithFixedDelay(this, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
            // Registered after the beat exists, so that a subscription the container has already given up on stops
            // this rather than leaving it running against a stream nobody reads.
            sink.onUnavailable(this::stop);
        }

        @Override
        public void run() {
            try {
                sink.keepAlive();
            } catch (Exception e) {
                // The subscriber is gone. Releasing the subscription is the container's to report, and it does;
                // there is nothing left here to keep alive.
                logger.debug("Stopped keeping subscription query [{}] alive.", requestIdentifier, e);
                stop();
            }
        }

        private void stop() {
            ScheduledFuture<?> scheduled = beat;
            if (scheduled != null) {
                scheduled.cancel(false);
            }
        }
    }
}
