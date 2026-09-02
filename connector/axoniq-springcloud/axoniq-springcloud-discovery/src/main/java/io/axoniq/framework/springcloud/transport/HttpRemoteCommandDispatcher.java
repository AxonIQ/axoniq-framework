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
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The {@link RemoteCommandDispatcher} that sends commands to other members over HTTP, and reads back their outcome.
 * <p>
 * {@link RestClient} is synchronous, so each dispatch runs on the supplied {@link Executor} rather than on the caller's
 * thread — the connector's {@code dispatch} must not block. A virtual-thread-per-task executor suits this well: the
 * work is a blocking HTTP round trip and nothing else, so the number of commands in flight is bounded by the far side's
 * capacity rather than by a thread pool of this member's choosing.
 * <p>
 * A failure to reach the member, or to read its reply, surfaces as a {@link MemberUnreachableException} — distinct
 * from a failure the member itself reported, which arrives inside the reply and surfaces as the exception that reply
 * describes. The connector uses that distinction to decide whether to hold the member responsible, so only the former
 * may use this exception.
 * <p>
 * Every dispatch is given a deadline. A member that stops answering without saying so — one that was killed, or
 * partitioned away — leaves a socket that reports nothing, which would otherwise hold the dispatch unresolved for as
 * long as the operating system allows. Reaching the deadline is reported as a failure to reach the member, so the
 * connector takes it out of the ring like any other member it could not reach.
 * <p>
 * Note that the deadline bounds when the dispatch <em>completes</em>, not the HTTP exchange itself: the thread running
 * the round trip stays blocked in the {@link RestClient} until the socket gives up. Connect and read timeouts belong
 * on the client's own request factory, and the Spring Boot autoconfiguration sets them on the {@code RestClient} it
 * contributes.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class HttpRemoteCommandDispatcher implements RemoteCommandDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(HttpRemoteCommandDispatcher.class);

    /**
     * How long a member is given to answer a command when no other deadline is configured.
     */
    public static final Duration DEFAULT_REPLY_TIMEOUT = Duration.ofSeconds(30);

    private final RestClient restClient;
    private final String commandEndpoint;
    private final Executor executor;
    private final @Nullable MessageConverter converter;
    private final Duration replyTimeout;

    /**
     * Constructs an {@code HttpRemoteCommandDispatcher} sending commands with the given {@code restClient}.
     *
     * @param restClient      the client used to send commands to other members
     * @param commandEndpoint the path, relative to a member's base URI, other members receive commands under. Must
     *                        match the path {@link SpringCloudCommandController} is mapped to across the cluster.
     * @param executor        the executor the blocking HTTP round trips run on
     * @param converter       the converter attached to received results for inline payload conversion, and used to
     *                        convert application-specific exception details, or {@code null} when none is available
     */
    public HttpRemoteCommandDispatcher(RestClient restClient,
                                       String commandEndpoint,
                                       Executor executor,
                                       @Nullable MessageConverter converter) {
        this(restClient, commandEndpoint, executor, converter, DEFAULT_REPLY_TIMEOUT);
    }

    /**
     * Constructs an {@code HttpRemoteCommandDispatcher} giving each command {@code replyTimeout} to be answered in.
     *
     * @param restClient      the client used to send commands to other members
     * @param commandEndpoint the path, relative to a member's base URI, other members receive commands under. Must
     *                        match the path {@link SpringCloudCommandController} is mapped to across the cluster.
     * @param executor        the executor the blocking HTTP round trips run on
     * @param converter       the converter attached to received results for inline payload conversion, and used to
     *                        convert application-specific exception details, or {@code null} when none is available
     * @param replyTimeout    how long a member is given to answer a command before it is treated as unreachable
     */
    public HttpRemoteCommandDispatcher(RestClient restClient,
                                       String commandEndpoint,
                                       Executor executor,
                                       @Nullable MessageConverter converter,
                                       Duration replyTimeout) {
        Objects.requireNonNull(replyTimeout, "The replyTimeout must not be null.");
        if (replyTimeout.isNegative() || replyTimeout.isZero()) {
            throw new IllegalArgumentException("The reply timeout must be positive, but was [" + replyTimeout + "].");
        }
        this.restClient = Objects.requireNonNull(restClient, "The restClient must not be null.");
        this.commandEndpoint = Objects.requireNonNull(commandEndpoint, "The commandEndpoint must not be null.");
        this.executor = Objects.requireNonNull(executor, "The executor must not be null.");
        this.converter = converter;
        this.replyTimeout = replyTimeout;
    }

    @Override
    public CompletableFuture<@Nullable CommandResultMessage> dispatch(Member member, CommandMessage command) {
        Objects.requireNonNull(member, "The member must not be null.");
        Objects.requireNonNull(command, "The command must not be null.");

        URI endpoint = member.endpoint();
        if (endpoint == null) {
            return CompletableFuture.failedFuture(new MemberUnreachableException(
                    "Member [" + member.name() + "] has no endpoint to send command [" + command.type() + "] to."
            ));
        }

        CommandDispatchRequest request;
        try {
            request = CommandConverter.convertCommandMessage(command);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        URI destination = UriComponentsBuilder.fromUri(endpoint).path(commandEndpoint).build().toUri();
        // orTimeout cancels its own scheduled timeout once the dispatch completes, so a command answered promptly
        // leaves nothing behind on the shared delayer.
        return CompletableFuture.supplyAsync(() -> post(destination, request, command), executor)
                                .orTimeout(replyTimeout.toMillis(), TimeUnit.MILLISECONDS)
                                .exceptionallyCompose(cause -> CompletableFuture.failedFuture(
                                        translateFailure(cause, command, destination)
                                ))
                                .thenApply(reply -> CommandConverter.convertReply(reply, converter));
    }

    private CommandDispatchReply post(URI destination, CommandDispatchRequest request, CommandMessage command) {
        logger.debug("Sending command [{}] to [{}]", command.type(), destination);
        CommandDispatchReply reply;
        try {
            reply = restClient.post()
                              .uri(destination)
                              .body(request)
                              .retrieve()
                              .body(CommandDispatchReply.class);
        } catch (Exception e) {
            throw new MemberUnreachableException(
                    "Could not send command [" + command.type() + "] to [" + destination + "].", e
            );
        }
        if (reply == null) {
            throw new MemberUnreachableException(
                    "Member at [" + destination + "] answered command [" + command.type() + "] with an empty body."
            );
        }
        return reply;
    }

    /**
     * Reports a command that ran out of time as a failure to reach the member, and passes every other failure through
     * unchanged.
     * <p>
     * A member that answers nothing is indistinguishable from one that cannot be reached, and the connector should
     * treat it the same way: take it out of the ring and let the next discovery round decide whether it is back.
     *
     * @param cause       the failure that ended the dispatch
     * @param command     the command that was being dispatched
     * @param destination the member the command was sent to
     * @return the failure to report to the connector
     */
    private Throwable translateFailure(Throwable cause, CommandMessage command, URI destination) {
        Throwable actual = cause instanceof CompletionException ? cause.getCause() : cause;
        if (actual instanceof TimeoutException) {
            return new MemberUnreachableException(
                    "Member at [%s] did not answer command [%s] within %s."
                            .formatted(destination, command.type(), replyTimeout), actual
            );
        }
        return actual;
    }
}
