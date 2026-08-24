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

package io.axoniq.framework.messaging.commandhandling.distributed;

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * The {@code CommandBusConnector} interface defines the contract for connecting multiple {@code CommandBus} instances.
 * It allows for the dispatching of commands across different command bus instances, whether they are local or remote.
 * <p>
 * One connector can be wrapped with another through the {@link DelegatingCommandBusConnector}, upon which more
 * functionality can be added, such as payload conversion or conversion.
 *
 * @author Allard Buijze
 * @author Mitchell Herrijgers
 * @author Steven van Beelen
 * @since 2.0.0
 */
public interface CommandBusConnector extends DescribableComponent {

    /**
     * Dispatches the given {@code command} to the appropriate command bus, which may be local or remote.
     *
     * @param command           The command message to dispatch.
     * @param processingContext The processing context for the command.
     * @return A {@link CompletableFuture} that will complete with the result of the command handling.
     */
    CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                     @Nullable ProcessingContext processingContext);

    /**
     * Subscribes to a command with the given {@code commandName} and a {@code loadFactor}.
     * <p>
     * Subscribing a {@code commandName} this connector is already subscribed to replaces the earlier subscription,
     * adopting the given {@code loadFactor}. A single {@link #unsubscribe(QualifiedName) unsubscribe} of that
     * {@code commandName} undoes any number of subscriptions to it.
     *
     * @param commandName The {@link QualifiedName} of the command to subscribe to.
     * @param loadFactor  The load factor for the command, which can be used to control the distribution of command
     *                    handling across multiple instances. The load factor should be a positive integer.
     * @return A {@code CompletableFuture} that completes successfully when this connector subscribed to the given
     * {@code commandName} with the given {@code loadFactor}.
     */
    CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor);

    /**
     * Unsubscribes from a command with the given {@code commandName}.
     * <p>
     * Undoes any number of {@link #subscribe(QualifiedName, int) subscriptions} to the given {@code commandName},
     * leaving no command of that name routed to this connector.
     *
     * @param commandName The {@link QualifiedName} of the command to unsubscribe from.
     * @return {@code true} if the unsubscription was successful, {@code false} otherwise.
     */
    boolean unsubscribe(QualifiedName commandName);

    /**
     * Registers a handler that will be called when an incoming command is received. The handler should process the
     * command and call the provided {@code ResultCallback} to indicate success or failure.
     *
     * @param handler A {@link BiConsumer} that takes a {@link CommandMessage} and a {@link ResultCallback}.
     */
    void onIncomingCommand(Handler handler);

    /**
     * A functional interface representing a handler for incoming command messages. The handler processes the command
     * and uses the provided {@link ResultCallback} to report the result.
     */
    @FunctionalInterface
    interface Handler {

        /**
         * Handles the incoming command message.
         *
         * @param commandMessage The command message to handle.
         * @param callback       The callback to invoke with the result of handling the command.
         */
        void handle(CommandMessage commandMessage, ResultCallback callback);
    }

    /**
     * A callback interface for handling the result of command processing. It provides methods to indicate success or
     * failure of command handling.
     */
    interface ResultCallback {

        /**
         * Called when the command processing is successful.
         *
         * @param resultMessage The result message containing the outcome of the command processing. If the message
         *                      handling yielded no result message, a {@code null} should be passed.
         */
        void onSuccess(@Nullable CommandResultMessage resultMessage);

        /**
         * Called when an error occurs during command processing.
         *
         * @param cause The exception that caused the error.
         */
        void onError(Throwable cause);
    }
}
