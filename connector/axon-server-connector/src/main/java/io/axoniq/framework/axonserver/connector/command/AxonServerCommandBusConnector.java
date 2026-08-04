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

package io.axoniq.framework.axonserver.connector.command;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.ConnectorLifecycle;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import org.axonframework.common.Assert;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.common.lifecycle.ShutdownLatch;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * An implementation of the {@link CommandBusConnector} that connects to an Axon Server instance to send and receive
 * commands. It uses the Axon Server gRPC API to communicate with the server.
 *
 * @author Allard Buijze
 * @author Mitchell Herrijgers
 * @author Jakob Hatzl
 * @since 5.0.0
 */
public class AxonServerCommandBusConnector implements CommandBusConnector, ConnectorLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(AxonServerCommandBusConnector.class);

    private final AxonServerConnection connection;
    private final String clientId;
    private final String componentName;
    private final @Nullable MessageConverter converter;
    private final Map<QualifiedName, Registration> subscriptions = new ConcurrentHashMap<>();
    private final ShutdownLatch shutdownLatch = new ShutdownLatch();
    private final ConcurrentHashMap<String, CompletableFuture<?>> commandsInProgress = new ConcurrentHashMap<>();

    private @Nullable Handler incomingHandler;

    /**
     * Creates a new {@code AxonServerConnector} that communicate with Axon Server using the provided
     * {@code connection}.
     *
     * @param connection    The {@code AxonServerConnection} to communicate to Axon Server with.
     * @param configuration The Axon Server configuration, used to retrieve (e.g.) the
     *                      {@link AxonServerConfiguration#getClientId()} to be set when
     *                      {@link #dispatch(CommandMessage, ProcessingContext) dispatching} commands.
     */
    public AxonServerCommandBusConnector(AxonServerConnection connection,
                                         AxonServerConfiguration configuration) {
        this(connection, configuration, null);
    }

    /**
     * Creates a new {@code AxonServerConnector} that communicate with Axon Server using the provided
     * {@code connection}.
     *
     * @param connection    The {@code AxonServerConnection} to communicate to Axon Server with.
     * @param configuration The Axon Server configuration, used to retrieve (e.g.) the
     *                      {@link AxonServerConfiguration#getClientId()} to be set when
     *                      {@link #dispatch(CommandMessage, ProcessingContext) dispatching} commands.
     * @param converter     The {@link MessageConverter} that should be attached to received {@link CommandMessage}s and
     *                      {@link CommandResultMessage} for inline payload conversion.
     */
    public AxonServerCommandBusConnector(AxonServerConnection connection,
                                         AxonServerConfiguration configuration,
                                         @Nullable MessageConverter converter) {
        this.connection = requireNonNull(connection, "The AxonServerConnection must not be null.");
        requireNonNull(configuration, "The AxonServerConfiguration must not be null.");
        this.clientId = configuration.getClientId();
        this.componentName = configuration.getComponentName();
        this.converter = converter;
    }

    /**
     * Starts the Axon Server {@link CommandBusConnector} implementation.
     */
    @Override
    public void start() {
        shutdownLatch.initialize();
        logger.trace("The AxonServerCommandBusConnector started.");
    }

    @Override
    public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                            @Nullable ProcessingContext processingContext) {
        shutdownLatch.ifShuttingDown("Cannot dispatch new commands as this bus is being shutdown");
        try (ShutdownLatch.ActivityHandle commandInTransit = shutdownLatch.registerActivity()) {
            return connection.commandChannel()
                             .sendCommand(CommandConverter.convertCommandMessage(command, clientId, componentName))
                             .thenCompose(commandResponse -> CommandConverter.convertCommandResponse(
                                     commandResponse,
                                     converter))
                             .whenComplete((commandResponse, throwable) -> commandInTransit.end());
        }
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor) {
        Assert.isTrue(loadFactor >= 0, () -> "Load factor must be greater than 0.");
        logger.debug("Subscribing to command [{}] with load factor [{}]", commandName, loadFactor);
        Registration registration = connection.commandChannel()
                                              .registerCommandHandler(this::handleCommand,
                                                                      loadFactor,
                                                                      commandName.name());
        subscriptions.put(commandName, registration);
        CompletableFuture<Void> completion = new CompletableFuture<>();
        registration.onAck(() -> completion.complete(null));
        return completion;
    }

    private CompletableFuture<CommandResponse> handleCommand(Command command) {
        logger.debug("Received incoming command [{}]", command.getName());
        String commandIdentifier = command.getMessageIdentifier();
        try {
            CompletableFuture<CommandResponse> result = new CompletableFuture<CommandResponse>()
                    .whenComplete((ignored, ignoredThrowable) -> commandsInProgress.remove(commandIdentifier));
            commandsInProgress.put(commandIdentifier, result);

            requireNonNull(incomingHandler, "incomingHandler not configured")
                    .handle(CommandConverter.convertCommand(command, converter),
                            futureResultCallback(result, command));
            return result;
        } catch (Exception e) {
            logger.error("Error processing incoming command: {}", command.getName(), e);
            commandsInProgress.remove(commandIdentifier);
            CompletableFuture<CommandResponse> errorResult = new CompletableFuture<>();
            errorResult.completeExceptionally(e);
            return errorResult;
        }
    }

    @Override
    public boolean unsubscribe(QualifiedName commandName) {
        Registration subscription = subscriptions.remove(commandName);
        if (subscription != null) {
            subscription.cancel();
            return true;
        }
        return false;
    }

    @Override
    public void onIncomingCommand(Handler handler) {
        this.incomingHandler = handler;
    }

    /**
     * Disconnect the command bus for receiving commands from Axon Server, by unsubscribing all registered command
     * handlers and waiting for in-flight commands to complete.
     * <p>
     * This shutdown operation is performed in the {@link Phase#INBOUND_COMMAND_CONNECTOR} phase.
     *
     * @return A completable future that completed once the {@link AxonServerConnection#commandChannel()} has prepared
     * disconnected and handled all in-flight incoming messages.
     */
    @Override
    public CompletableFuture<Void> disconnect() {
        if (!connection.isConnected()) {
            return CompletableFuture.completedFuture(null);
        }
        logger.trace("Disconnecting the AxonServerCommandBusConnector.");
        CompletableFuture<?>[] inFlight = commandsInProgress.values().stream()
                                                            .map(future -> (CompletableFuture<?>) future)
                                                            .toArray(CompletableFuture[]::new);
        return connection.commandChannel()
                         .prepareDisconnect()
                         .thenCompose(ignored -> CompletableFuture.allOf(inFlight))
                         .thenRun(connection::disconnect);
    }

    /**
     * Shutdown the command bus asynchronously for dispatching commands to Axon Server. This process will wait for
     * dispatched commands which have not received a response yet. This shutdown operation is performed in the
     * {@link Phase#OUTBOUND_COMMAND_CONNECTORS} phase.
     *
     * @return A completable future that is resolved once all command dispatching activities are completed.
     */
    @Override
    public CompletableFuture<Void> shutdownDispatching() {
        logger.trace("Shutting down dispatching of AxonServerCommandBusConnector.");
        return shutdownLatch.initiateShutdown();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("connection", connection);
        descriptor.describeProperty("clientId", clientId);
        descriptor.describeProperty("componentName", componentName);
    }

    private CommandBusConnector.ResultCallback futureResultCallback(
            CompletableFuture<CommandResponse> result,
            Command command
    ) {
        return new CommandBusConnector.ResultCallback() {

            @Override
            public void onSuccess(@Nullable CommandResultMessage resultMessage) {
                logger.debug("Command [{}] completed successfully with result [{}]",
                             command.getName(),
                             resultMessage);
                result.complete(CommandConverter.convertResultMessage(resultMessage, command.getMessageIdentifier()));
            }

            @Override
            public void onError(Throwable cause) {
                logger.info("Command [{}] raised an exception [{}]",
                            command.getName(),
                            cause.getMessage());
                result.complete(CommandConverter.convertErrorResponse(
                        clientId, command.getMessageIdentifier(), cause, converter
                ));
            }
        };
    }
}
