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

package io.axoniq.framework.springcloud.command;

import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector.Handler;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import org.axonframework.messaging.commandhandling.CommandDispatchException;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.NoHandlerForCommandException;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Turns a {@link CommandDispatchRequest} received from another member into an invocation of this application's local
 * command handler, and its outcome back into a {@link CommandDispatchReply}.
 * <p>
 * This invoker exists as a component of its own, separate from
 * {@link SpringCloudCommandController}, for two reasons. It breaks what would otherwise be a construction cycle: the
 * controller is a Spring bean created before the connector, while the {@link CommandBusConnector.Handler handler} it
 * must invoke only becomes available once {@code DistributedCommandBus} registers one on the connector, later still.
 * The connector {@link #bind(CommandBusConnector.Handler) binds} the handler here when it receives it. It also keeps
 * the whole receiving path testable without standing up a web stack — the controller that remains is nothing but an
 * HTTP mapping.
 * <p>
 * A request that arrives before a handler is bound is answered with a
 * {@link CommandErrorCode#NO_HANDLER_FOR_COMMAND} reply rather than an HTTP error. The dispatching member then sees a
 * transient {@link org.axonframework.messaging.commandhandling.NoHandlerForCommandException}, which is exactly the
 * situation: this member is still starting up, and will be able to handle the command shortly.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class IncomingCommandInvoker {

    private static final Logger logger = LoggerFactory.getLogger(IncomingCommandInvoker.class);

    private final Supplier<String> memberName;
    private final @Nullable MessageConverter converter;
    private final AtomicReference<@Nullable Handler> handler = new AtomicReference<>();

    /**
     * Constructs an {@code IncomingCommandInvoker} reporting failures as originating from the member the given
     * {@code memberName} supplies.
     * <p>
     * Taken as a supplier because a member is not named until it has registered with discovery, which happens after
     * this invoker is built. Resolving it per failure reports the name this member is actually known by, rather than
     * the provisional one it had at start-up.
     *
     * @param memberName supplies the name identifying this application in replies it sends, used to point at the
     *                   member a failure originated on
     * @param converter  the converter attached to received commands for inline payload conversion, and used to
     *                   serialize application-specific exception details, or {@code null} when none is available
     */
    public IncomingCommandInvoker(Supplier<String> memberName, @Nullable MessageConverter converter) {
        this.memberName = Objects.requireNonNull(memberName, "The memberName must not be null.");
        this.converter = converter;
    }

    /**
     * Binds the {@code handler} that incoming commands are to be invoked on, replacing any previously bound one.
     * <p>
     * Called by the connector when {@code DistributedCommandBus} registers its handler through
     * {@link CommandBusConnector#onIncomingCommand(CommandBusConnector.Handler)}.
     *
     * @param handler the handler to invoke incoming commands on
     */
    public void bind(CommandBusConnector.Handler handler) {
        this.handler.set(Objects.requireNonNull(handler, "The handler must not be null."));
    }

    /**
     * Handles the given {@code request}, received from another member of the cluster.
     * <p>
     * The returned future always completes successfully: a failure while handling the command is reported <em>in</em>
     * the reply, not as a failed future, so the controller can answer with it.
     *
     * @param request the request received from another member
     * @return a future completing with the reply to send back
     */
    public CompletableFuture<CommandDispatchReply> handle(CommandDispatchRequest request) {
        Objects.requireNonNull(request, "The request must not be null.");
        Handler boundHandler = handler.get();
        if (boundHandler == null) {
            logger.info("Received command [{}] before a handler was registered on this member. Reporting it as "
                                + "unhandled so the dispatching member can retry.", request.type());
            return CompletableFuture.completedFuture(CommandConverter.convertErrorResult(
                    new NoHandlerForCommandException(
                            "This member has not registered a command handler yet, as it is still starting up."
                    ),
                    request.identifier(),
                    memberName.get(),
                    converter
            ));
        }

        CommandMessage command;
        try {
            command = CommandConverter.convertRequest(request, converter);
        } catch (Exception e) {
            logger.warn("Could not read incoming command [{}] of type [{}].",
                        request.identifier(), request.type(), e);
            // The command never reached a handler, which is a dispatch failure rather than an execution failure, and
            // the dispatching member needs that distinction to know that retrying the same bytes will not help.
            return CompletableFuture.completedFuture(CommandConverter.convertErrorResult(
                    new CommandDispatchException(
                            "Could not read incoming command of type [" + request.type() + "].", e
                    ),
                    request.identifier(), memberName.get(), converter
            ));
        }

        CompletableFuture<CommandDispatchReply> reply = new CompletableFuture<>();
        try {
            boundHandler.handle(command, new ReplyingResultCallback(reply, request.identifier()));
        } catch (Exception e) {
            logger.warn("Could not hand incoming command [{}] to the local handler.", command.type(), e);
            reply.complete(CommandConverter.convertErrorResult(e, request.identifier(), memberName.get(), converter));
        }
        return reply;
    }

    private class ReplyingResultCallback implements CommandBusConnector.ResultCallback {

        private final CompletableFuture<CommandDispatchReply> reply;
        private final String requestIdentifier;

        private ReplyingResultCallback(CompletableFuture<CommandDispatchReply> reply, String requestIdentifier) {
            this.reply = reply;
            this.requestIdentifier = requestIdentifier;
        }

        @Override
        public void onSuccess(@Nullable CommandResultMessage resultMessage) {
            try {
                reply.complete(CommandConverter.convertResultMessage(resultMessage, requestIdentifier));
            } catch (Exception e) {
                logger.warn("Could not write the result of command [{}] to a reply.", requestIdentifier, e);
                reply.complete(CommandConverter.convertErrorResult(e, requestIdentifier, memberName.get(), converter));
            }
        }

        @Override
        public void onError(Throwable cause) {
            reply.complete(CommandConverter.convertErrorResult(cause, requestIdentifier, memberName.get(), converter));
        }
    }
}
