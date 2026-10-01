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

import io.axoniq.framework.springcloud.routing.Member;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QueueMessageStream;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Sends queries to other members over HTTP, reading the responses they stream back as Server-Sent Events.
 * <p>
 * Reading a stream blocks for as long as the answering member keeps it open, so each query is read on the given
 * executor rather than on the thread that dispatched it. A virtual-thread executor suits this: the threads spend
 * their lives waiting on a socket.
 * <p>
 * Responses are buffered in a queue of a fixed size. A member answering faster than this application consumes fills
 * that queue, and the query then fails rather than growing the buffer until memory runs out. Where a bounded buffer
 * is the wrong trade for a particular application, the size is configurable.
 * <p>
 * Every query is given a deadline, after which its responses are no longer waited for. This is a backstop rather than
 * the usual way a query ends: the answering member closes its own stream first, on a shorter timeout of its own. It
 * exists for the member that stops answering without saying so — one that was killed, or partitioned away — whose
 * socket would otherwise never report anything, leaving the query unanswered and its thread parked for as long as the
 * operating system allows.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class HttpRemoteQueryDispatcher implements RemoteQueryDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(HttpRemoteQueryDispatcher.class);

    /**
     * The number of responses buffered per query when no other size is configured.
     */
    public static final int DEFAULT_BUFFER_SIZE = 1024;

    /**
     * How long a query's responses are waited for when no other deadline is configured.
     */
    public static final Duration DEFAULT_RESPONSE_TIMEOUT = Duration.ofMinutes(6);

    /**
     * How long a subscription may hear nothing at all from the answering member before it is given up on, when no
     * other window is configured.
     * <p>
     * Not a deadline on the subscription, which lasts as long as the subscriber wants it to, but on silence: the
     * answering member sends a keep-alive well inside this window, so a subscription that hears nothing for the whole
     * of it is one whose member is gone.
     * <p>
     * Silence is measured by looking every half a window, so a departed member is given up on somewhere between one
     * and one and a half times this. It is the order of magnitude that matters, not the exact moment.
     */
    public static final Duration DEFAULT_SUBSCRIPTION_INACTIVITY_TIMEOUT = Duration.ofSeconds(60);

    private final RestClient restClient;
    private final String queryEndpoint;
    private final Executor executor;
    private final MessageConverter converter;
    private final int bufferSize;
    private final Duration responseTimeout;
    private final Duration subscriptionInactivityTimeout;
    private final ScheduledExecutorService scheduler;

    /**
     * Constructs an {@code HttpRemoteQueryDispatcher} buffering {@link #DEFAULT_BUFFER_SIZE} responses per query.
     *
     * @param restClient    the client sending the queries
     * @param queryEndpoint the path other members receive queries under
     * @param executor      the executor each query's response stream is read on
     * @param converter     the converter reading the data of each response event, and attached to the responses read
     *                      from it for inline payload conversion
     */
    public HttpRemoteQueryDispatcher(RestClient restClient,
                                     String queryEndpoint,
                                     Executor executor,
                                     MessageConverter converter) {
        this(restClient, queryEndpoint, executor, converter, DEFAULT_BUFFER_SIZE,
             DEFAULT_RESPONSE_TIMEOUT, DEFAULT_SUBSCRIPTION_INACTIVITY_TIMEOUT, defaultScheduler());
    }

    /**
     * Constructs an {@code HttpRemoteQueryDispatcher} buffering {@code bufferSize} responses per query.
     *
     * @param restClient      the client sending the queries
     * @param queryEndpoint   the path other members receive queries under
     * @param executor        the executor each query's response stream is read on
     * @param converter       the converter reading the data of each response event, and attached to the responses
     *                        read from it for inline payload conversion
     * @param bufferSize      how many responses to a single query are held before the answering member outpacing
     *                        this application fails the query.
     * @param responseTimeout how long a query's responses are waited for before it is given up on. Should exceed the
     *                        timeout the answering members apply to their own response streams, so that a member
     *                        which is merely slow ends the query itself rather than being given up on here.
     * @param subscriptionInactivityTimeout
     *                        how long a subscription may hear nothing at all from the answering member before it is
     *                        given up on. Must exceed the interval at which members keep an idle subscription alive.
     * @param scheduler       runs the deadline of each query and the silence check of each subscription. Cancelled
     *                        deadlines are removed from it, so a query that completes quickly does not leave its
     *                        responses reachable until the deadline would have elapsed.
     */
    public HttpRemoteQueryDispatcher(RestClient restClient,
                                     String queryEndpoint,
                                     Executor executor,
                                     MessageConverter converter,
                                     int bufferSize,
                                     Duration responseTimeout,
                                     Duration subscriptionInactivityTimeout,
                                     ScheduledExecutorService scheduler) {
        if (bufferSize < 1) {
            throw new IllegalArgumentException("The buffer size must be at least 1, but was [" + bufferSize + "].");
        }
        this.restClient = Objects.requireNonNull(restClient, "The restClient must not be null.");
        this.queryEndpoint = Objects.requireNonNull(queryEndpoint, "The queryEndpoint must not be null.");
        this.executor = Objects.requireNonNull(executor, "The executor must not be null.");
        this.converter = Objects.requireNonNull(converter, "The converter must not be null.");
        this.bufferSize = bufferSize;
        this.responseTimeout = requirePositive(responseTimeout, "response timeout");
        this.subscriptionInactivityTimeout = requirePositive(subscriptionInactivityTimeout,
                                                             "subscription inactivity timeout");
        this.scheduler = Objects.requireNonNull(scheduler, "The scheduler must not be null.");
    }

    private static Duration requirePositive(Duration value, String description) {
        Objects.requireNonNull(value, "The " + description + " must not be null.");
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException("The " + description + " must be positive, but was [" + value + "].");
        }
        return value;
    }

    /**
     * Returns the scheduler used when none is supplied, which removes a deadline from its queue as soon as it is
     * cancelled rather than holding the query's responses until it would have elapsed.
     *
     * @return the scheduler deadlines run on when none is supplied
     */
    private static ScheduledExecutorService defaultScheduler() {
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, task -> {
            Thread thread = new Thread(task, "axoniq-springcloud-query-deadline");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }

    @Override
    public MessageStream<QueryResponseMessage> dispatch(Member member, QueryMessage query) {
        Objects.requireNonNull(member, "The member must not be null.");
        Objects.requireNonNull(query, "The query must not be null.");

        URI endpoint = member.endpoint();
        if (endpoint == null) {
            return MessageStream.failed(new QueryDispatchException(
                    "Member [" + member.name() + "] has no endpoint to send query [" + query.type() + "] to."
            ));
        }
        QueryDispatchRequest request;
        try {
            request = QueryConverter.convertQueryMessage(query);
        } catch (Exception e) {
            return MessageStream.failed(e);
        }

        URI destination = UriComponentsBuilder.fromUri(endpoint).path(queryEndpoint).build().toUri();
        QueueMessageStream<QueryResponseMessage> responses =
                new QueueMessageStream<>(new ArrayBlockingQueue<>(bufferSize));
        AtomicReference<@Nullable InputStream> body = new AtomicReference<>();
        AtomicBoolean released = new AtomicBoolean();

        ScheduledFuture<?> deadline = scheduleDeadline(destination, query, responses, body, released);
        executor.execute(() -> read(destination, request, query, responses, body, released, deadline));

        // Releasing the stream stops the answering member from being read any further. Closing the body is what
        // interrupts the read, which is blocked on the socket and reachable no other way.
        return responses.onClose(() -> {
            released.set(true);
            deadline.cancel(false);
            close(body.get());
        });
    }

    private void read(URI destination,
                      QueryDispatchRequest request,
                      QueryMessage query,
                      QueueMessageStream<QueryResponseMessage> responses,
                      AtomicReference<@Nullable InputStream> body,
                      AtomicBoolean released,
                      ScheduledFuture<?> deadline) {
        logger.debug("Sending query [{}] to [{}]", query.type(), destination);
        try {
            restClient.post()
                      .uri(destination)
                      .accept(MediaType.TEXT_EVENT_STREAM)
                      .body(request)
                      .exchange((outgoing, response) -> {
                          if (response.getStatusCode().isError()) {
                              throw new IllegalStateException(
                                      "Member at [" + destination + "] answered with status "
                                              + response.getStatusCode() + "."
                              );
                          }
                          InputStream stream = response.getBody();
                          body.set(stream);
                          if (!released.get()) {
                              ServerSentEventReader.read(stream, event -> onEvent(event, responses));
                          }
                          return null;
                      });
            responses.seal();
        } catch (Exception e) {
            if (released.get()) {
                // The consumer released the stream, so the read ending is the intended outcome rather than a failure.
                responses.seal();
            } else {
                responses.sealExceptionally(new QueryDispatchException(
                        "Could not read the responses to query [" + query.type() + "] from [" + destination + "].", e
                ));
            }
        } finally {
            deadline.cancel(false);
        }
    }

    /**
     * Schedules the abandoning of a query whose responses did not arrive in time.
     * <p>
     * Sealing before closing the body means this is what the query fails with: the read's own failure, raised when the
     * body it is blocked on is closed, finds the stream already sealed and adds nothing.
     */
    private ScheduledFuture<?> scheduleDeadline(URI destination,
                                                QueryMessage query,
                                                QueueMessageStream<QueryResponseMessage> responses,
                                                AtomicReference<@Nullable InputStream> body,
                                                AtomicBoolean released) {
        return scheduler.schedule(
                () -> {
                    logger.info("Giving up on query [{}] to [{}], which was not answered within {}.",
                                query.type(), destination, responseTimeout);
                    released.set(true);
                    responses.sealExceptionally(new QueryDispatchException(
                            "Member at [%s] did not answer query [%s] within %s."
                                    .formatted(destination, query.type(), responseTimeout)
                    ));
                    close(body.get());
                },
                responseTimeout.toMillis(), TimeUnit.MILLISECONDS
        );
    }

    @Override
    public MessageStream<QueryResponseMessage> openSubscriptionQueryUpdateStream(Member member,
                                                                                 QueryMessage query,
                                                                                 int updateBufferSize,
                                                                                 SubscriptionListener listener) {
        Objects.requireNonNull(member, "The member must not be null.");
        Objects.requireNonNull(query, "The query must not be null.");
        Objects.requireNonNull(listener, "The listener must not be null.");
        if (updateBufferSize < 1) {
            throw new IllegalArgumentException(
                    "The update buffer size must be at least 1, but was [" + updateBufferSize + "]."
            );
        }

        URI endpoint = member.endpoint();
        if (endpoint == null) {
            return MessageStream.failed(new QueryDispatchException(
                    "Member [" + member.name() + "] has no endpoint to subscribe query [" + query.type() + "] on."
            ));
        }
        SubscriptionQueryRequest request;
        try {
            request = QueryConverter.convertSubscriptionMessage(query, updateBufferSize);
        } catch (Exception e) {
            return MessageStream.failed(e);
        }

        URI destination = UriComponentsBuilder.fromUri(endpoint)
                                              .path(queryEndpoint)
                                              .path(SpringCloudQueryController.SUBSCRIPTION_PATH)
                                              .build()
                                              .toUri();
        QueueMessageStream<QueryResponseMessage> updates =
                new QueueMessageStream<>(new ArrayBlockingQueue<>(updateBufferSize));
        AtomicReference<@Nullable InputStream> body = new AtomicReference<>();
        AtomicBoolean released = new AtomicBoolean();
        AtomicLong lastActivity = new AtomicLong(System.nanoTime());

        ScheduledFuture<?> silence = scheduleSilenceCheck(destination, query, updates, body, released, lastActivity);
        executor.execute(() -> readSubscription(destination, request, query, updates,
                                                body, released, lastActivity, silence, listener));

        // Releasing the subscription stops the member from being read any further. Closing the body is what
        // interrupts the read, which is blocked on the socket and reachable no other way.
        return updates.onClose(() -> {
            released.set(true);
            silence.cancel(false);
            close(body.get());
        });
    }

    private void readSubscription(URI destination,
                                  SubscriptionQueryRequest request,
                                  QueryMessage query,
                                  QueueMessageStream<QueryResponseMessage> updates,
                                  AtomicReference<@Nullable InputStream> body,
                                  AtomicBoolean released,
                                  AtomicLong lastActivity,
                                  ScheduledFuture<?> silence,
                                  SubscriptionListener listener) {
        logger.debug("Opening the update stream of query [{}] on [{}]", query.type(), destination);
        try {
            restClient.post()
                      .uri(destination)
                      .accept(MediaType.TEXT_EVENT_STREAM)
                      .body(request)
                      .exchange((outgoing, response) -> {
                          if (response.getStatusCode().isError()) {
                              throw new IllegalStateException(
                                      "Member at [" + destination + "] answered with status "
                                              + response.getStatusCode() + "."
                              );
                          }
                          // The member registers the subscription before it answers at all, so a response means the
                          // subscription is in place and the initial result may now be asked for.
                          listener.opened();
                          // Wrapped so that the keep-alive a member sends over an otherwise idle subscription counts
                          // as the sign of life it is meant to be, even though the reader discards it.
                          InputStream stream = new ActivityRecordingInputStream(response.getBody(), lastActivity);
                          body.set(stream);
                          if (!released.get()) {
                              ServerSentEventReader.read(
                                      stream,
                                      event -> onUpdate(event, updates, request.updateBufferSize(), listener)
                              );
                          }
                          return null;
                      });
            updates.seal();
        } catch (Exception e) {
            if (released.get()) {
                // The subscriber released it, so the read ending is the intended outcome rather than a failure.
                updates.seal();
            } else {
                updates.sealExceptionally(new QueryDispatchException(
                        "Lost the subscription for query [" + query.type() + "] on [" + destination + "].", e
                ));
            }
        } finally {
            silence.cancel(false);
        }
    }

    /**
     * Schedules the abandoning of a subscription that has gone quiet.
     * <p>
     * A subscription has no deadline of its own, since it lasts for as long as the subscriber wants it to. What it
     * cannot survive is the answering member disappearing without the socket reporting it, which reads as an
     * indefinitely idle stream. The answering member sends a keep-alive well inside this window, so silence for the
     * whole of it means the member is gone rather than merely quiet.
     */
    private ScheduledFuture<?> scheduleSilenceCheck(URI destination,
                                                    QueryMessage query,
                                                    QueueMessageStream<QueryResponseMessage> updates,
                                                    AtomicReference<@Nullable InputStream> body,
                                                    AtomicBoolean released,
                                                    AtomicLong lastActivity) {
        long periodMillis = Math.max(1, subscriptionInactivityTimeout.toMillis() / 2);
        return scheduler.scheduleWithFixedDelay(
                () -> {
                    if (System.nanoTime() - lastActivity.get() < subscriptionInactivityTimeout.toNanos()) {
                        return;
                    }
                    logger.info("Giving up on the subscription for query [{}] on [{}], which sent nothing for {}.",
                                query.type(), destination, subscriptionInactivityTimeout);
                    released.set(true);
                    updates.sealExceptionally(new QueryDispatchException(
                            "Member at [%s] sent nothing for subscription query [%s] within %s."
                                    .formatted(destination, query.type(), subscriptionInactivityTimeout)
                    ));
                    close(body.get());
                },
                periodMillis, periodMillis, TimeUnit.MILLISECONDS
        );
    }

    private void onUpdate(ServerSentEvent event,
                          QueueMessageStream<QueryResponseMessage> updates,
                          int updateBufferSize,
                          SubscriptionListener listener) {
        switch (event.event()) {
            case QueryConverter.UPDATE_EVENT -> {
                QueryDispatchResponse update = parse(event.data(), QueryDispatchResponse.class);
                if (!updates.offer(QueryConverter.convertResponse(update, converter), Context.empty())) {
                    throw new IllegalStateException(
                            ("The answering member produced more than %d updates ahead of this application consuming "
                                    + "them. Consume the updates sooner, or raise the update buffer size.")
                                    .formatted(updateBufferSize)
                    );
                }
            }
            case QueryConverter.COMPLETE_EVENT -> {
                // The member says the subscription has run its course, which is more than this member's part in it
                // ending. Reported before sealing, so the subscriber stops waiting on every other member too.
                listener.completed();
                updates.seal();
            }
            case QueryConverter.ERROR_EVENT ->
                    updates.sealExceptionally(QueryConverter.convertError(parse(event.data(),
                                                                               QueryDispatchFailure.class)));
            default -> logger.debug("Ignoring event of unrecognised type [{}].", event.event());
        }
    }

    /**
     * Records when the answering member was last heard from, whatever it sent.
     * <p>
     * Wraps the response body rather than the reader, so that a keep-alive comment counts as activity even though
     * the reader discards it without reporting an event.
     */
    private static final class ActivityRecordingInputStream extends FilterInputStream {

        private final AtomicLong lastActivity;

        private ActivityRecordingInputStream(InputStream delegate, AtomicLong lastActivity) {
            super(delegate);
            this.lastActivity = lastActivity;
        }

        @Override
        public int read() throws IOException {
            int read = super.read();
            if (read >= 0) {
                lastActivity.set(System.nanoTime());
            }
            return read;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int read = super.read(buffer, offset, length);
            if (read > 0) {
                lastActivity.set(System.nanoTime());
            }
            return read;
        }
    }

    private void onEvent(ServerSentEvent event, QueueMessageStream<QueryResponseMessage> responses) {
        switch (event.event()) {
            case QueryConverter.RESPONSE_EVENT -> {
                QueryDispatchResponse response = parse(event.data(), QueryDispatchResponse.class);
                if (!responses.offer(QueryConverter.convertResponse(response, converter), Context.empty())) {
                    throw new IllegalStateException(
                            ("The answering member produced more than %d responses ahead of this application "
                                    + "consuming them. Consume the responses sooner, or raise the buffer size.")
                                    .formatted(bufferSize)
                    );
                }
            }
            case QueryConverter.ERROR_EVENT -> {
                QueryDispatchFailure failure = parse(event.data(), QueryDispatchFailure.class);
                responses.sealExceptionally(QueryConverter.convertError(failure));
            }
            default -> logger.debug("Ignoring event of unrecognised type [{}].", event.event());
        }
    }

    private <T> T parse(String data, Class<T> type) {
        T parsed;
        try {
            parsed = converter.convert(data, type);
        } catch (Exception e) {
            throw new IllegalStateException("Could not read a [" + type.getSimpleName() + "] from the stream.", e);
        }
        if (parsed == null) {
            throw new IllegalStateException("Read an empty [" + type.getSimpleName() + "] from the stream.");
        }
        return parsed;
    }

    private static void close(@Nullable Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            logger.debug("Could not close the response stream; it is being abandoned anyway.", e);
        }
    }
}
