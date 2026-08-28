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
import org.axonframework.messaging.commandhandling.CommandDispatchException;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * The {@link RemoteCommandDispatcher} that sends commands to other members over HTTP, and reads back their outcome.
 * <p>
 * {@link RestClient} is synchronous, so each dispatch runs on the supplied {@link Executor} rather than on the caller's
 * thread — the connector's {@code dispatch} must not block. A virtual-thread-per-task executor suits this well: the
 * work is a blocking HTTP round trip and nothing else, so the number of commands in flight is bounded by the far side's
 * capacity rather than by a thread pool of this member's choosing.
 * <p>
 * A failure to reach the member, or to read its reply, surfaces as a {@link CommandDispatchException} — distinct from
 * the failure of a handler that did run, which arrives inside the reply. The connector uses that distinction to decide
 * whether to hold the member responsible.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class HttpRemoteCommandDispatcher implements RemoteCommandDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(HttpRemoteCommandDispatcher.class);

    private final RestClient restClient;
    private final String commandEndpoint;
    private final Executor executor;
    private final @Nullable MessageConverter converter;

    /**
     * Constructs an {@code HttpRemoteCommandDispatcher} sending commands with the given {@code restClient}.
     *
     * @param restClient      The client used to send commands to other members.
     * @param commandEndpoint The path, relative to a member's base URI, other members receive commands under. Must
     *                        match the path {@link SpringCloudCommandController} is mapped to across the cluster.
     * @param executor        The executor the blocking HTTP round trips run on.
     * @param converter       The converter attached to received results for inline payload conversion, and used to
     *                        convert application-specific exception details, or {@code null} when none is available.
     */
    public HttpRemoteCommandDispatcher(RestClient restClient,
                                       String commandEndpoint,
                                       Executor executor,
                                       @Nullable MessageConverter converter) {
        this.restClient = Objects.requireNonNull(restClient, "The restClient cannot be null.");
        this.commandEndpoint = Objects.requireNonNull(commandEndpoint, "The commandEndpoint cannot be null.");
        this.executor = Objects.requireNonNull(executor, "The executor cannot be null.");
        this.converter = converter;
    }

    @Override
    public CompletableFuture<@Nullable CommandResultMessage> dispatch(Member member, CommandMessage command) {
        Objects.requireNonNull(member, "The member cannot be null.");
        Objects.requireNonNull(command, "The command cannot be null.");

        URI endpoint = member.endpoint();
        if (endpoint == null) {
            return CompletableFuture.failedFuture(new CommandDispatchException(
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
        return CompletableFuture.supplyAsync(() -> post(destination, request, command), executor)
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
            throw new CommandDispatchException(
                    "Could not send command [" + command.type() + "] to [" + destination + "].", e
            );
        }
        if (reply == null) {
            throw new CommandDispatchException(
                    "Member at [" + destination + "] answered command [" + command.type() + "] with an empty body."
            );
        }
        return reply;
    }
}
