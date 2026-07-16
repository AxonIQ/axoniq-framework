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
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import org.axonframework.common.Assert;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

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
 * @since 5.0.0
 */
public class AxonServerCommandBusConnector extends AbstractAxonServerCommandBusConnector {

    private final AxonServerConnection connection;
    private final Map<QualifiedName, Registration> subscriptions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<?>> commandsInProgress = new ConcurrentHashMap<>();

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
        super(clientId(configuration), componentName(configuration), converter);
        this.connection = requireNonNull(connection, "The AxonServerConnection must not be null.");
    }

    @Override
    public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                            @Nullable ProcessingContext processingContext) {
        return doDispatch(command, connection);
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor) {
        Assert.isTrue(loadFactor >= 0, () -> "Load factor must be greater than 0.");
        logger.debug("Subscribing to command [{}] with load factor [{}]", commandName, loadFactor);
        return doSubscribe(commandName,
                           loadFactor,
                           connection,
                           subscriptions,
                           commandsInProgress,
                           Command::getMessageIdentifier);
    }

    @Override
    public boolean unsubscribe(QualifiedName commandName) {
        return doUnsubscribe(commandName, subscriptions);
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
        logger.trace("Disconnecting the {}.", getClass().getSimpleName());
        return doDisconnect(connection, commandsInProgress.values());
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        super.describeTo(descriptor);
        descriptor.describeProperty("connection", connection);
    }


    private static String clientId(final AxonServerConfiguration config) {
        return requireNonNull(config, "AxonServerConfiguration may not be null").getClientId();
    }

    private static String componentName(final AxonServerConfiguration config) {
        return requireNonNull(config, "AxonServerConfiguration may not be null").getComponentName();
    }
}
