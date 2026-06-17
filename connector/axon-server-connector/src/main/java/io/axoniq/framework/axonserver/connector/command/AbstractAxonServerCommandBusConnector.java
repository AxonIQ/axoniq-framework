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
import io.axoniq.framework.axonserver.connector.api.ConnectorLifecycle;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.common.lifecycle.ShutdownLatch;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Shared Axon Server command-bus connector behavior.
 * <p>
 * Concrete connectors provide the tenant or context specific state. This base class keeps the common lifecycle, command
 * conversion and handler callback plumbing in one place so that single-context and multi-tenant connectors can reuse
 * the same behavior.
 *
 * @author Jan Galinski
 * @since 5.2.0
 */
@Internal
public abstract class AbstractAxonServerCommandBusConnector implements CommandBusConnector, ConnectorLifecycle {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final String clientId;
    protected final String componentName;
    protected final @Nullable MessageConverter converter;
    protected final ShutdownLatch shutdownLatch = new ShutdownLatch();
    protected @Nullable Handler incomingHandler;

    protected AbstractAxonServerCommandBusConnector(String clientId,
                                                    String componentName,
                                                    @Nullable MessageConverter converter) {
        this.clientId = requireNonNull(clientId, "The clientId must not be null.");
        this.componentName = requireNonNull(componentName, "The componentName must not be null.");
        this.converter = converter;
    }

    /**
     * Starts the Axon Server {@link CommandBusConnector} implementation.
     */
    @Override
    public void start() {
        shutdownLatch.initialize();
        logger.trace("The {} started.", getClass().getSimpleName());
    }

    @Override
    public void onIncomingCommand(Handler handler) {
        this.incomingHandler = handler;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("clientId", clientId);
        descriptor.describeProperty("componentName", componentName);
    }

    /**
     * Sends the given {@code command} to the given {@code connection}.
     *
     * @param command the command to dispatch
     * @param connection the connection/context to use
     * @return result message future
     */
    protected final CompletableFuture<CommandResultMessage> doDispatch(CommandMessage command,
                                                                       AxonServerConnection connection) {
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

    protected final CompletableFuture<Void> doSubscribe(QualifiedName commandName,
                                                        int loadFactor,
                                                        AxonServerConnection connection,
                                                        Map<QualifiedName, Registration> subscriptions,
                                                        ConcurrentHashMap<String, CompletableFuture<?>> commandsInProgress,
                                                        Function<Command, String> commandKeyFactory) {
        Registration registration = connection.commandChannel()
                                              .registerCommandHandler(command ->
                                                                              handleCommand(command,
                                                                                            commandsInProgress,
                                                                                            commandKeyFactory),
                                                                      loadFactor,
                                                                      commandName.name());
        subscriptions.put(commandName, registration);
        CompletableFuture<Void> completion = new CompletableFuture<>();
        registration.onAck(() -> completion.complete(null));
        return completion;
    }

    protected final boolean doUnsubscribe(QualifiedName commandName,
                                          Map<QualifiedName, Registration> subscriptions) {
        Registration subscription = subscriptions.remove(commandName);
        if (subscription != null) {
            subscription.cancel();
            return true;
        }
        return false;
    }

    protected final CompletableFuture<CommandResponse> handleCommand(Command command,
                                                                     ConcurrentHashMap<String, CompletableFuture<?>> commandsInProgress,
                                                                     Function<Command, String> commandKeyFactory) {
        logger.debug("Received incoming command [{}]", command.getName());
        String commandKey = commandKeyFactory.apply(command);
        try {
            CompletableFuture<CommandResponse> result = new CompletableFuture<CommandResponse>()
                    .whenComplete((ignored, ignoredThrowable) -> commandsInProgress.remove(commandKey));
            commandsInProgress.put(commandKey, result);

            requireNonNull(incomingHandler, "incomingHandler not configured")
                    .handle(CommandConverter.convertCommand(command, converter), futureResultCallback(
                            result,
                            command
                    ));

            return result;
        } catch (Exception e) {
            logger.error("Error processing incoming command: {}", command.getName(), e);
            commandsInProgress.remove(commandKey);
            CompletableFuture<CommandResponse> errorResult = new CompletableFuture<>();
            errorResult.completeExceptionally(e);
            return errorResult;
        }
    }

    protected final CompletableFuture<Void> doDisconnect(AxonServerConnection connection,
                                                         Collection<? extends CompletableFuture<?>> commandsInProgress) {
        if (!connection.isConnected()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<?>[] inFlight = commandsInProgress.stream()
                                                            .map(future -> (CompletableFuture<?>) future)
                                                            .toArray(CompletableFuture[]::new);
        return connection.commandChannel()
                         .prepareDisconnect()
                         .thenCompose(ignored -> CompletableFuture.allOf(inFlight))
                         .thenRun(() -> {
                         });
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
        logger.trace("Shutting down dispatching of the {}.", getClass().getSimpleName());
        return shutdownLatch.initiateShutdown();
    }

    protected CommandBusConnector.ResultCallback futureResultCallback(
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
                result.completeExceptionally(cause);
            }
        };
    }
}
