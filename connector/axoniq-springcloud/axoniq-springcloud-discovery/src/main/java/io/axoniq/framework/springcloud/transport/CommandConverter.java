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

import io.axoniq.framework.messaging.commandhandling.distributed.PayloadConvertingCommandBusConnector;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.commandhandling.CommandDispatchException;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.NoHandlerForCommandException;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.HandlerExecutionException;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.RemoteExceptionDescription;
import org.axonframework.messaging.core.RemoteHandlingException;
import org.axonframework.messaging.core.RemoteNonTransientHandlingException;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.UUID;

import static io.axoniq.framework.springcloud.transport.WireCodec.copyOf;
import static io.axoniq.framework.springcloud.transport.WireCodec.decode;
import static io.axoniq.framework.springcloud.transport.WireCodec.descriptionsOf;
import static io.axoniq.framework.springcloud.transport.WireCodec.encode;
import static io.axoniq.framework.springcloud.transport.WireCodec.messageOf;
import static io.axoniq.framework.springcloud.transport.WireCodec.serializedDetailsOf;

/**
 * Converts commands and their outcomes between {@link CommandMessage}/{@link CommandResultMessage} and the
 * {@link CommandDispatchRequest}/{@link CommandDispatchReply} carried over HTTP.
 * <p>
 * Payloads are expected to be {@code byte[]} by the time they reach this converter, which is what the
 * {@code PayloadConvertingCommandBusConnector} wrapped around the connector guarantees. They are Base64-encoded so
 * they survive JSON.
 * <p>
 * Marked {@link Internal} as the wire format it produces is specific to this connector, and both ends of any one
 * cluster are expected to run the same version of it.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
@Internal
final class CommandConverter {

    /**
     * Local stack traces are suppressed on reconstructed remote failures: the stack of the thread that read the reply
     * says nothing about where the command actually failed, and the descriptions carried in the reply do.
     */
    private static final boolean WRITABLE_STACK_TRACE = false;
    private static final String PAYLOAD_DECORATOR = PayloadConvertingCommandBusConnector.class.getSimpleName();

    /**
     * Converts the given {@code command} into the request to send to another member.
     *
     * @param command the command to send
     * @return the wire representation of the given {@code command}
     * @throws IllegalArgumentException when the given {@code command}'s payload is not a {@code byte[]}
     */
    public static CommandDispatchRequest convertCommandMessage(CommandMessage command) {
        return new CommandDispatchRequest(
                command.identifier(),
                command.type().toString(),
                encode(WireCodec.payloadAsBytes(command.payload(), command.payloadType(), PAYLOAD_DECORATOR)),
                copyOf(command.metadata()),
                command.routingKey().orElse(null),
                command.priority().isPresent() ? command.priority().getAsInt() : null
        );
    }

    /**
     * Converts the given {@code request}, received from another member, into the command to handle locally.
     *
     * @param request   the request received from another member
     * @param converter the converter to attach to the resulting command for inline payload conversion, or {@code null}
     *                  when none is available
     * @return the command the given {@code request} represents
     */
    public static CommandMessage convertRequest(CommandDispatchRequest request, @Nullable Converter converter) {
        return new GenericCommandMessage(
                new GenericMessage(
                        request.identifier(),
                        MessageType.fromString(request.type()),
                        decode(request.payload()),
                        copyOf(request.metadata())
                ),
                request.routingKey(),
                request.priority()
        ).withConverter(converter);
    }

    /**
     * Converts the given {@code resultMessage} into the reply to send back to the member that dispatched the command
     * identified by {@code requestIdentifier}.
     *
     * @param resultMessage     the result of handling the command, or {@code null} when the handler returned none
     * @param requestIdentifier the identifier of the command being replied to
     * @return the wire representation of the given {@code resultMessage}
     * @throws IllegalArgumentException when the given {@code resultMessage}'s payload is not a {@code byte[]}
     */
    public static CommandDispatchReply convertResultMessage(@Nullable CommandResultMessage resultMessage,
                                                            String requestIdentifier) {
        if (resultMessage == null) {
            return emptyReply(requestIdentifier);
        }
        Object payload = resultMessage.payload();
        if (payload == null) {
            return CommandDispatchReply.result(resultMessage.identifier(),
                                               requestIdentifier,
                                               resultMessage.type().toString(),
                                               null,
                                               copyOf(resultMessage.metadata()));
        }
        return CommandDispatchReply.result(resultMessage.identifier(),
                                           requestIdentifier,
                                           resultMessage.type().toString(),
                                           encode(WireCodec.payloadAsBytes(payload,
                                                                           resultMessage.payloadType(),
                                                                           PAYLOAD_DECORATOR)),
                                           copyOf(resultMessage.metadata()));
    }

    /**
     * Converts the given {@code cause}, thrown while handling the command identified by {@code requestIdentifier},
     * into the reply to send back to the member that dispatched it.
     *
     * @param cause             the exception thrown while handling the command
     * @param requestIdentifier the identifier of the command being replied to
     * @param origin            the name of this member, reported as where the failure originated
     * @param converter         the converter used to serialize application-specific exception details, or {@code null}
     *                          when none is available
     * @return the wire representation of the given {@code cause}
     */
    public static CommandDispatchReply convertErrorResult(Throwable cause,
                                                          String requestIdentifier,
                                                          String origin,
                                                          @Nullable Converter converter) {
        byte[] details = serializedDetailsOf(cause, converter);
        Object rawDetails = HandlerExecutionException.resolveDetails(cause).orElse(null);
        return CommandDispatchReply.error(
                UUID.randomUUID().toString(),
                requestIdentifier,
                CommandErrorCode.classify(cause),
                messageOf(cause),
                origin,
                descriptionsOf(cause),
                details == null || rawDetails == null ? null : rawDetails.getClass().getName(),
                encode(details)
        );
    }

    /**
     * Converts the given {@code reply}, received from the member that handled a command, into the result to complete
     * the dispatching future with.
     *
     * @param reply     the reply received from the member that handled the command
     * @param converter the converter to attach to the result, and to lazily convert exception details with, or
     *                  {@code null} when none is available
     * @return the result the given {@code reply} represents, or {@code null} when the handler returned none
     * @throws org.axonframework.common.AxonException reconstructed from the given {@code reply} when it reports a
     *                                               failure
     */
    public static @Nullable CommandResultMessage convertReply(CommandDispatchReply reply,
                                                              @Nullable Converter converter) {
        if (reply.isError()) {
            throw convertError(reply, converter);
        }
        String type = reply.type();
        if (type == null) {
            return null;
        }
        return new GenericCommandResultMessage(new GenericMessage(
                reply.identifier(),
                MessageType.fromString(type),
                decode(reply.payload()),
                copyOf(reply.metadata())
        )).withConverter(converter);
    }

    /**
     * Reconstructs the exception the given {@code reply} reports.
     *
     * @param reply     the reply reporting a failure
     * @param converter the converter to lazily convert application-specific exception details with, or {@code null}
     *                  when none is available
     * @return the exception the given {@code reply} reports
     */
    private static RuntimeException convertError(CommandDispatchReply reply, @Nullable Converter converter) {
        CommandErrorCode errorCode = reply.errorCode();
        String message = reply.errorMessage() == null
                ? "The member handling the command reported a failure without a message."
                : reply.errorMessage();
        String described = reply.errorOrigin() == null ? message : message + " [origin: " + reply.errorOrigin() + "]";
        List<String> descriptions = reply.errorDetails().isEmpty() ? List.of(message) : reply.errorDetails();
        byte[] details = decode(reply.errorDetailsPayload());

        return switch (errorCode) {
            case NO_HANDLER_FOR_COMMAND -> new NoHandlerForCommandException(described);
            case COMMAND_DISPATCH_ERROR -> new CommandDispatchException(described);
            case COMMAND_EXECUTION_ERROR -> new CommandExecutionException(
                    described,
                    new RemoteHandlingException(new RemoteExceptionDescription(descriptions)),
                    details,
                    converter,
                    WRITABLE_STACK_TRACE
            );
            case COMMAND_EXECUTION_NON_TRANSIENT_ERROR -> new CommandExecutionException(
                    described,
                    new RemoteNonTransientHandlingException(new RemoteExceptionDescription(descriptions, true)),
                    details,
                    converter,
                    WRITABLE_STACK_TRACE
            );
            case null -> new CommandDispatchException(
                    "The member handling the command reported a failure of an unrecognised kind: " + described
            );
        };
    }

    /**
     * Constructs a reply reporting that the handler produced no result for the command identified by
     * {@code requestIdentifier}.
     *
     * @param requestIdentifier the identifier of the command being replied to
     * @return a reply carrying neither a result nor a failure
     */
    public static CommandDispatchReply emptyReply(String requestIdentifier) {
        return CommandDispatchReply.noResult(UUID.randomUUID().toString(), requestIdentifier);
    }

    private CommandConverter() {
        // Utility class
    }
}
