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

import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.springcloud.routing.Member;
import io.axoniq.framework.springcloud.shared.SpringCloudAxoniqAddon;
import io.axoniq.framework.springcloud.shared.SpringCloudMemberRegistry;
import io.axoniq.license.entitlement.EntitlementManager;
import io.axoniq.license.entitlement.EntitlementMessageType;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.common.lifecycle.ShutdownLatch;
import org.axonframework.messaging.commandhandling.CommandDispatchException;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.NoHandlerForCommandException;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link CommandBusConnector} distributing commands across the nodes discovered through Spring Cloud Discovery,
 * .
 * <p>
 * With no server to route for it, this connector routes itself. Every dispatch resolves a {@link Member} from the
 * {@link SpringCloudMemberRegistry}'s consistent-hash ring, using the command's routing key and name, and then either
 * hands the command to this application's own handler or sends it to the resolved member over HTTP. That is the shape
 * of the Axon Framework 5 contract: {@code DistributedCommandBus} passes a dispatch straight through, so routing is
 * the connector's job.
 * <p>
 * Two consequences of that are worth being explicit about:
 * <ul>
 *     <li>A command routed to this application is delivered through the {@link Handler} registered by
 *     {@code DistributedCommandBus}, never by calling a local {@code CommandBus} directly. That handler owns the
 *     priority-ordered executor and the local segment, so bypassing it would lose command priority and the bus's own
 *     interception.</li>
 *     <li>{@link #subscribe(QualifiedName, int)} completes immediately, recording the subscription locally and
 *     publishing the updated capabilities. It deliberately does not wait for other members to observe the change:
 *     {@code DistributedCommandBus} joins this future on the subscribing thread, and other members learn of the
 *     subscription on their next discovery heartbeat regardless.</li>
 * </ul>
 * This connector is wired by {@link io.axoniq.framework.springcloud.SpringCloudConfigurationEnhancer}, wrapped in a
 * {@code PayloadConvertingCommandBusConnector} that converts payloads to {@code String} on the way out. It is not
 * meant to be constructed directly by application code.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class SpringCloudCommandBusConnector implements CommandBusConnector {

    private static final Logger logger = LoggerFactory.getLogger(SpringCloudCommandBusConnector.class);

    private final SpringCloudMemberRegistry registry;
    private final IncomingCommandInvoker invoker;
    private final RemoteCommandDispatcher dispatcher;
    private final @Nullable MessageConverter converter;
    private final EntitlementManager entitlementManager;

    private final Map<QualifiedName, Integer> subscriptions = new ConcurrentHashMap<>();
    private final ShutdownLatch shutdownLatch = new ShutdownLatch();

    private volatile @Nullable Handler incomingHandler;

    /**
     * Constructs a {@code SpringCloudCommandBusConnector} routing with the given {@code registry}.
     *
     * @param registry   the registry holding the consistent-hash ring commands are routed with
     * @param invoker    the component invoking the local handler for commands from other members
     * @param dispatcher the dispatcher sending commands to other members
     * @param converter  the converter attached to commands routed to this application, so that a locally routed
     *                   command carries the same conversion capability as one that travelled over the wire, or
     *                   {@code null} when none is available
     */
    public SpringCloudCommandBusConnector(SpringCloudMemberRegistry registry,
                                          IncomingCommandInvoker invoker,
                                          RemoteCommandDispatcher dispatcher,
                                          @Nullable MessageConverter converter) {
        this(registry, invoker, dispatcher, converter, EntitlementManager.INSTANCE);
        EntitlementManager.INSTANCE.registerAddon(SpringCloudAxoniqAddon.class);
    }

    /**
     * Package-private constructor allowing an alternative {@link EntitlementManager} to be injected.
     * <p>
     * Marked {@link Internal} because production code must use
     * {@link #SpringCloudCommandBusConnector(SpringCloudMemberRegistry, IncomingCommandInvoker,
     * RemoteCommandDispatcher, MessageConverter)}, which registers the addon and claims against
     * {@link EntitlementManager#INSTANCE}. This constructor exists so tests need not touch that singleton.
     *
     * @param registry           the registry holding the consistent-hash ring commands are routed with
     * @param invoker            the component invoking the local handler for commands from other members
     * @param dispatcher         the dispatcher sending commands to other members
     * @param converter          the converter attached to commands routed to this application, or {@code null} when
     *                           none is available
     * @param entitlementManager the entitlement manager dispatched commands are claimed against
     */
    @Internal
    SpringCloudCommandBusConnector(SpringCloudMemberRegistry registry,
                                   IncomingCommandInvoker invoker,
                                   RemoteCommandDispatcher dispatcher,
                                   @Nullable MessageConverter converter,
                                   EntitlementManager entitlementManager) {
        this.registry = Objects.requireNonNull(registry, "The registry must not be null.");
        this.invoker = Objects.requireNonNull(invoker, "The invoker must not be null.");
        this.dispatcher = Objects.requireNonNull(dispatcher, "The dispatcher must not be null.");
        this.converter = converter;
        this.entitlementManager = Objects.requireNonNull(entitlementManager,
                                                         "The entitlementManager must not be null.");
    }

    /**
     * Starts this connector, allowing commands to be dispatched through it.
     * <p>
     * Performed in the {@link Phase#INBOUND_COMMAND_CONNECTOR} phase.
     */
    public void start() {
        shutdownLatch.initialize();
        logger.debug("The SpringCloudCommandBusConnector started.");
    }

    @Override
    public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                            @Nullable ProcessingContext processingContext) {
        Objects.requireNonNull(command, "The command must not be null.");
        shutdownLatch.ifShuttingDown("Cannot dispatch new commands as this connector is shutting down.");

        QualifiedName commandName = command.type().qualifiedName();
        // Falling back to the identifier keeps an unrouted command dispatchable: it lands on an arbitrary but valid
        // member, which is the correct behaviour for a command that declared no entity to be routed by.
        String routingKey = command.routingKey().orElseGet(command::identifier);

        Optional<Member> destination = registry.findCommandDestination(routingKey, commandName);
        if (destination.isEmpty()) {
            return CompletableFuture.failedFuture(new NoHandlerForCommandException(command));
        }

        entitlementManager.claimMessage(SpringCloudAxoniqAddon.IDENTIFIER, EntitlementMessageType.COMMAND, 1);

        Member member = destination.get();
        ShutdownLatch.ActivityHandle inTransit = shutdownLatch.registerActivity();
        try {
            CompletableFuture<@Nullable CommandResultMessage> result = member.local()
                    ? handleLocally(command)
                    : dispatchRemotely(member, command);
            return result.whenComplete((ignored, cause) -> inTransit.end());
        } catch (Exception e) {
            inTransit.end();
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<@Nullable CommandResultMessage> handleLocally(CommandMessage command) {
        Handler handler = incomingHandler;
        if (handler == null) {
            return CompletableFuture.failedFuture(new CommandDispatchException(
                    "This member resolved as the destination for command [" + command.type()
                            + "], but no command handler is registered on the connector yet."
            ));
        }
        logger.debug("Handling command [{}] on this member.", command.type());

        CompletableFuture<@Nullable CommandResultMessage> result = new CompletableFuture<>();
        handler.handle(withConverterAttached(command), new ResultCallback() {
            @Override
            public void onSuccess(@Nullable CommandResultMessage resultMessage) {
                result.complete(resultMessage);
            }

            @Override
            public void onError(Throwable cause) {
                result.completeExceptionally(cause);
            }
        });
        return result;
    }

    private CompletableFuture<@Nullable CommandResultMessage> dispatchRemotely(Member member, CommandMessage command) {
        return dispatcher.dispatch(member, command)
                         .whenComplete((result, cause) -> {
                             if (cause != null && isUnreachable(cause)) {
                                 registry.markUnreachable(member);
                             }
                         });
    }

    /**
     * Indicates whether the given {@code cause} means the member could not be reached, as opposed to the member having
     * reported a failure of its own.
     * <p>
     * Only the former says anything about the member's availability. Anything the member reported — a handler that
     * threw, or a command it could not read — is an outcome it produced while perfectly reachable, and taking it out of
     * the ring for that would move a failing command onto every other member in turn until the ring is empty.
     * <p>
     * The distinction rests on {@link MemberUnreachableException}, which only the transport raises, and never on
     * {@link CommandDispatchException} itself: a member reporting that it could not dispatch a command answers with
     * {@link io.axoniq.framework.springcloud.command.CommandErrorCode#COMMAND_DISPATCH_ERROR}, which is reconstructed
     * as a plain {@code CommandDispatchException} on this side and must not evict it.
     *
     * @param cause the failure that completed a remote dispatch
     * @return {@code true} when the member could not be reached, {@code false} otherwise
     */
    private static boolean isUnreachable(Throwable cause) {
        Throwable actual = cause instanceof CompletionException && cause.getCause() != null
                ? cause.getCause()
                : cause;
        return actual instanceof MemberUnreachableException;
    }

    /**
     * Returns the given {@code command} with this connector's converter attached.
     * <p>
     * A command that travelled over the wire is reconstructed with the converter attached to it, so that handlers and
     * interceptors can convert its payload inline. A command routed to this application never leaves the JVM and would
     * otherwise arrive without one, making the behaviour of a handler depend on where routing happened to land. This
     * closes that gap.
     *
     * @param command the command routed to this application
     * @return the given {@code command} with this connector's converter attached
     */
    private CommandMessage withConverterAttached(CommandMessage command) {
        if (converter == null) {
            return command;
        }
        return new GenericCommandMessage(
                new GenericMessage(command.identifier(), command.type(), command.payload(), command.metadata()),
                command.routingKey().orElse(null),
                command.priority().isPresent() ? command.priority().getAsInt() : null
        ).withConverter(converter);
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor) {
        Objects.requireNonNull(commandName, "The commandName must not be null.");
        if (loadFactor < 0) {
            // Reported through the returned future rather than thrown, so that a caller composing on it sees the
            // failure at all. Nothing is recorded, so the connector is left as it was.
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "The load factor cannot be negative, but was [" + loadFactor + "]."
            ));
        }
        logger.debug("Subscribing to command [{}] with load factor [{}].", commandName, loadFactor);
        subscriptions.put(commandName, loadFactor);
        publishCapabilities();
        return FutureUtils.emptyCompletedFuture();
    }

    @Override
    public boolean unsubscribe(QualifiedName commandName) {
        Objects.requireNonNull(commandName, "The commandName must not be null.");
        if (subscriptions.remove(commandName) == null) {
            return false;
        }
        logger.debug("Unsubscribing from command [{}].", commandName);
        publishCapabilities();
        return true;
    }

    /**
     * Publishes the commands currently subscribed to as this member's capabilities.
     * <p>
     * A ring position is per member, not per command name, so one load factor is published for the member as a whole.
     * {@code DistributedCommandBus} subscribes every command name with the same, bus-wide load factor, so taking the
     * highest of the recorded values yields exactly that value; should per-command load factors ever arrive, the most
     * demanding subscription is the safe one to size this member by.
     */
    private void publishCapabilities() {
        Set<QualifiedName> commands = Set.copyOf(subscriptions.keySet());
        int loadFactor = subscriptions.values().stream().max(Comparator.naturalOrder()).orElse(0);
        registry.publishLocalCommands(loadFactor, commands);
    }

    @Override
    public void onIncomingCommand(Handler handler) {
        Objects.requireNonNull(handler, "The handler must not be null.");
        this.incomingHandler = handler;
        invoker.bind(handler);
    }

    /**
     * Stops advertising the commands this member handles, so that other members stop routing them here.
     * <p>
     * Performed in the {@link Phase#INBOUND_COMMAND_CONNECTOR} phase. Incoming commands arrive over HTTP, so it is the
     * web container's own graceful shutdown that stops new requests; this connector's part is to publish empty
     * capabilities, so that members still running route around it on their next discovery round rather than sending
     * commands into a closing container.
     *
     * @return a future that completes once this member no longer advertises any command
     */
    public CompletableFuture<Void> disconnect() {
        logger.debug("Disconnecting the SpringCloudCommandBusConnector.");
        subscriptions.clear();
        publishCapabilities();
        return FutureUtils.emptyCompletedFuture();
    }

    /**
     * Stops dispatching new commands, and waits for the ones already dispatched to be answered.
     * <p>
     * Performed in the {@link Phase#OUTBOUND_COMMAND_CONNECTORS} phase.
     *
     * @return a future that completes once every dispatched command has been answered
     */
    public CompletableFuture<Void> shutdownDispatching() {
        logger.debug("Shutting down dispatching of the SpringCloudCommandBusConnector.");
        return shutdownLatch.initiateShutdown();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("registry", registry);
        descriptor.describeProperty("dispatcher", dispatcher);
        descriptor.describeProperty("subscriptions", subscriptions.keySet().stream()
                                                                 .map(QualifiedName::toString)
                                                                 .sorted()
                                                                 .toList());
    }
}
