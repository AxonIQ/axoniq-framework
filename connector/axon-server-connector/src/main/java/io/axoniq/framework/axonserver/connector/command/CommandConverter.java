/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.command;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.ProcessingKey;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.shared.ErrorCode;
import io.axoniq.framework.axonserver.connector.shared.ExceptionConverter;
import io.axoniq.framework.axonserver.connector.shared.MetadataConverter;
import io.axoniq.framework.axonserver.connector.util.ProcessingInstructionUtils;
import org.axonframework.common.StringUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.framework.axonserver.connector.shared.MetadataConverter.convertMetadataValuesToGrpc;
import static io.axoniq.framework.axonserver.connector.util.ProcessingInstructionUtils.createProcessingInstruction;
import static io.axoniq.framework.axonserver.connector.util.ProcessingInstructionUtils.priority;
import static org.axonframework.common.ObjectUtils.getOrDefault;

/**
 * Utility class to convert commands during
 * {@link AxonServerCommandBusConnector#dispatch(CommandMessage, ProcessingContext) dispatching} and handling of
 * {@link AxonServerCommandBusConnector#subscribe(QualifiedName, int) subscribed} command handlers in the
 * {@link AxonServerCommandBusConnector}.
 * <p>
 * The operations {@link #convertCommandMessage(CommandMessage, String, String) convert}
 * {@link CommandMessage CommandMessages} and
 * {@link #convertCommandResponse(CommandResponse, Converter, MessageType) convert}
 * {@link CommandResponse CommandResponses} are used during dispatching. The operations
 * {@link #convertCommand(Command, Converter) convert} {@link Command Commands} and
 * {@link #convertResultMessage(CommandResultMessage, String) convert} result messages are used during handling.
 * <p>
 * This utility class is marked as {@link Internal} as it is specific for the {@link AxonServerCommandBusConnector}.
 *
 * @author Mitchell Herrijgers
 * @author Steven van Beelen
 * @since 5.0.0
 */
@Internal
public final class CommandConverter {

    /**
     * Converts the given {@code command} into a {@link Command} for
     * {@link AxonServerCommandBusConnector#dispatch(CommandMessage, ProcessingContext) dispatching}.
     * <p>
     * Will set the {@link ProcessingKey#ROUTING_KEY routing key} and {@link ProcessingKey#PRIORITY priority} when
     * present on the given {@code command}.
     *
     * @param command       the command message to convert to a {@link Command}
     * @param clientId      the identifier of this application, as specific in the {@link AxonServerConfiguration}
     * @param componentName the name of this application, as specific in the {@link AxonServerConfiguration}
     * @return the given {@code command} converted to a {@link Command}
     */
    public static Command convertCommandMessage(CommandMessage command,
                                                String clientId,
                                                String componentName) {
        Object payload = command.payload();
        if (!(payload instanceof byte[] payloadAsBytes)) {
            throw new IllegalArgumentException(
                    "Payload must be of type byte[] for AxonServerConnector, but was: "
                            + command.payloadType().getName()
                            + ", consider using a Converter-based CommandBusConnector"
            );
        }
        Command.Builder builder = Command.newBuilder();
        addRoutingKey(builder, command);
        addPriority(builder, command);

        return builder.setClientId(clientId)
                      .setComponentName(componentName)
                      .setMessageIdentifier(command.identifier())
                      .setName(command.type().name())
                      .putAllMetaData(MetadataConverter.convertGrpcToMetadataValues(command.metadata()))
                      .setPayload(SerializedObject.newBuilder()
                                                  .setData(ByteString.copyFrom(payloadAsBytes))
                                                  .setType(command.type().name())
                                                  .setRevision(command.type().version())
                                                  .build())
                      .build();
    }

    /**
     * Converts the given {@code commandResponse} to a {@link CommandResultMessage}, wrapped in a
     * {@link CompletableFuture} for convenience when dealing with {@link CommandResponse CommandResponses} during
     * {@link AxonServerCommandBusConnector#dispatch(CommandMessage, ProcessingContext) dispatching}.
     *
     * @param commandResponse    the command response to convert to a {@link CommandResultMessage}
     * @param converter          the converter to use for payload conversion in the resulting
     *                           {@link CommandResultMessage}
     * @param fallbackResultType the {@link MessageType} to use for the resulting {@link CommandResultMessage} when the
     *                           response carries no payload; typically the dispatched {@link CommandMessage}'s own
     *                           {@link CommandMessage#type() type}, as there is no result-specific type to derive one
     *                           from
     * @return the {@code commandResponse} converted to a {@link CommandResultMessage}, wrapped in a
     * {@link CompletableFuture} for convenience. The resulting {@link CommandResultMessage} carries a {@code null}
     * payload when the response carries no result, which is the case when the response's payload has no type, or is
     * typed as {@link Void} without data; the future completes exceptionally when the response holds an error
     */
    public static CompletableFuture<CommandResultMessage> convertCommandResponse(
            CommandResponse commandResponse,
            @Nullable Converter converter,
            MessageType fallbackResultType
    ) {
        SerializedObject commandResponsePayload = commandResponse.getPayload();
        if (commandResponse.hasErrorMessage()) {
            return CompletableFuture.failedFuture(ExceptionConverter.convertToAxonException(
                    commandResponse.getErrorCode(),
                    commandResponse.getErrorMessage(),
                    commandResponsePayload,
                    converter
            ));
        }

        MessageType resultType = commandResponsePayload.getType().isEmpty()
                ? fallbackResultType
                : new MessageType(commandResponsePayload.getType(), revisionOrDefault(commandResponsePayload));
        byte[] payload = isEmptyResult(commandResponsePayload) ? null : commandResponsePayload.getData().toByteArray();
        Map<String, String> metadata = convertMetadataValuesToGrpc(commandResponse.getMetaDataMap());
        return CompletableFuture.completedFuture(new GenericCommandResultMessage(new GenericMessage(
                commandResponse.getMessageIdentifier(),
                resultType,
                payload,
                metadata
        )).withConverter(converter));
    }

    /**
     * Converts the given {@code command} into a {@link CommandMessage} for handling in
     * {@link AxonServerCommandBusConnector#subscribe(QualifiedName, int) subscribed} command handlers.
     *
     * @param command   the command to convert to a {@link CommandMessage}
     * @param converter the converter to use for payload conversion in the resulting {@link CommandMessage}
     * @return the given {@code command} converted into a {@link CommandMessage}
     */
    public static CommandMessage convertCommand(Command command, @Nullable Converter converter) {
        SerializedObject commandPayload = command.getPayload();
        int priority = priority(command.getProcessingInstructionsList());
        String routingKey = ProcessingInstructionUtils.routingKey(command.getProcessingInstructionsList());
        String revision = revisionOrDefault(commandPayload);

        return new GenericCommandMessage(
                new GenericMessage(
                        command.getMessageIdentifier(),
                        new MessageType(commandPayload.getType(), revision),
                        commandPayload.getData().toByteArray(),
                        convertMetadataValuesToGrpc(command.getMetaDataMap())
                ),
                routingKey,
                priority
        ).withConverter(converter);
    }

    /**
     * Converts the given {@code resultMessage} into a {@link CommandResponse}, using the given
     * {@code requestIdentifier} to correlate the {@link Command} that led to this {@link CommandResponse}.
     * <p>
     * When the {@code resultMessage} carries a {@code null} payload, as is the case for command handlers that do not
     * return a result, the {@link CommandResponse} carries a payload of the {@code resultMessage}'s type without data.
     * For a handler that returns nothing, that type is {@link Void}, which dispatchers recognize as an empty result.
     *
     * @param resultMessage     the result message to convert to a {@link CommandResponse}
     * @param requestIdentifier the identifier correlating the {@link CommandResponse} to the {@link Command} that led
     *                          to the response
     * @return a {@link CommandResponse} based on the given {@code resultMessage} and {@code requestIdentifier}
     */
    public static CommandResponse convertResultMessage(CommandResultMessage resultMessage,
                                                       String requestIdentifier) {
        Object payload = resultMessage.payload();
        String messageId = getOrDefault(resultMessage.identifier(), UUID.randomUUID().toString());
        CommandResponse.Builder responseBuilder =
                CommandResponse.newBuilder()
                               .setMessageIdentifier(messageId)
                               .putAllMetaData(MetadataConverter.convertGrpcToMetadataValues(resultMessage.metadata()))
                               .setRequestIdentifier(requestIdentifier);

        if (payload != null && !(payload instanceof byte[])) {
            throw new IllegalArgumentException(
                    "Payload must be of type byte[] for AxonServerConnector, but was: %s, consider using a Converter-based CommandBusConnector".
                            formatted(resultMessage.payloadType().getName())
            );
        }
        byte[] payloadAsBytes = (byte[]) Objects.requireNonNullElse(payload, new byte[0]);
        return responseBuilder.setPayload(SerializedObject.newBuilder()
                                                          .setType(resultMessage.type().name())
                                                          .setRevision(resultMessage.type().version())
                                                          .setData(ByteString.copyFrom(payloadAsBytes)))
                              .build();
    }

    private static void addRoutingKey(Command.Builder builder, CommandMessage command) {
        command.routingKey().ifPresent(routingKey -> {
            var instruction = createProcessingInstruction(ProcessingKey.ROUTING_KEY, routingKey);
            builder.addProcessingInstructions(instruction);
        });
    }

    private static void addPriority(Command.Builder builder, CommandMessage command) {
        command.priority().ifPresent(priority -> {
            var instruction = createProcessingInstruction(ProcessingKey.PRIORITY, priority);
            builder.addProcessingInstructions(instruction);
        });
    }

    /**
     * Returns whether the given {@code payload} of a {@link CommandResponse} describes an empty result.
     * <p>
     * That is the case when the {@code payload} has no type, or when it is typed as {@link Void} without data, which
     * is how handlers describe a {@code null} result. The type decides rather than the data alone, as results like an
     * empty {@code String} legitimately convert to empty data.
     *
     * @param payload the {@link SerializedObject} of a {@link CommandResponse} to validate
     * @return {@code true} if the {@code payload} describes an empty result, {@code false} otherwise
     */
    private static boolean isEmptyResult(SerializedObject payload) {
        String type = payload.getType();
        return type.isEmpty() || (Void.class.getName().equals(type) && payload.getData().isEmpty());
    }

    /**
     * Return the {@link SerializedObject#getRevision()} or {@link MessageType#DEFAULT_VERSION} if the revision is
     * {@code empty} or {@code null}.
     *
     * @param payload the {@link SerializedObject} to resolve the revision from
     * @return the revision or the {@link MessageType#DEFAULT_VERSION} if the revision is {@code empty} or {@code null}
     */
    private static String revisionOrDefault(SerializedObject payload) {
        return StringUtils.nonEmptyOrNull(payload.getRevision())
                ? payload.getRevision()
                : MessageType.DEFAULT_VERSION;
    }

    /**
     * Converts the given {@code cause} into a {@link CommandResponse} carrying an error, using the given
     * {@code requestIdentifier} to correlate the {@link Command} that led to this {@link CommandResponse}.
     * <p>
     * Used when a {@link AxonServerCommandBusConnector#subscribe(QualifiedName, int) subscribed} command handler
     * completes a command exceptionally.
     *
     * @param clientId          the identifier of this application, as specified in the {@link AxonServerConfiguration},
     *                          used as the location reported in the resulting error message
     * @param requestIdentifier the identifier correlating the {@link CommandResponse} to the {@link Command} that led
     *                          to the response
     * @param cause             the exception to convert into the error carried by the {@link CommandResponse}
     * @param converter         the converter to use for serializing application-specific exception details onto the
     *                          resulting {@link CommandResponse}, or {@code null} if no such conversion is available
     * @return a {@link CommandResponse} carrying the error derived from the given {@code cause}
     */
    public static CommandResponse convertErrorResponse(String clientId,
                                                       String requestIdentifier,
                                                       Throwable cause,
                                                       @Nullable Converter converter) {
        ErrorCode errorCode = ErrorCode.getCommandExecutionErrorCode(cause);
        CommandResponse.Builder responseBuilder =
                CommandResponse.newBuilder()
                               .setMessageIdentifier(UUID.randomUUID().toString())
                               .setRequestIdentifier(requestIdentifier)
                               .setErrorCode(errorCode.errorCode())
                               .setErrorMessage(ExceptionConverter.convertToErrorMessage(clientId, errorCode, cause));
        SerializedObject detailsPayload = ExceptionConverter.convertToDetails(cause, converter);
        if (detailsPayload != null) {
            responseBuilder.setPayload(detailsPayload);
        }
        return responseBuilder.build();
    }

    private CommandConverter() {
        // Utility class
    }
}
