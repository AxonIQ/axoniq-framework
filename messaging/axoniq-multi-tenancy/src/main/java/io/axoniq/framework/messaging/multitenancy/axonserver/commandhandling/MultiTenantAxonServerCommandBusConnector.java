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

package io.axoniq.framework.messaging.multitenancy.axonserver.commandhandling;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.api.ConnectorLifecycle;
import io.axoniq.framework.axonserver.connector.command.AxonServerCommandBusConnector;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException.tenantNotResolved;

/**
 * Multi-tenant Axon Server {@link CommandBusConnector}.
 * <p>
 * The connector composes one {@link AxonServerCommandBusConnector} per {@link TenantDescriptor}, each owning its own
 * Axon Server connection, command handler subscriptions, and in-flight command tracking. Command subscription state
 * and the incoming-command handler are replayed onto newly registered tenants.
 *
 * Internal, because the connector is wired by {@link
 * io.axoniq.framework.messaging.multitenancy.axonserver.configuration.AxonServerMultiTenancyConfigurationDefaults} and
 * reached through the {@link CommandBusConnector} component, never constructed by an application itself.
 *
 * @author Jan Galinski
 * @author Jakob Hatzl
 * @since 5.3.0
 */
@Internal
public class MultiTenantAxonServerCommandBusConnector
        implements CommandBusConnector, MultiTenantAwareComponent, ConnectorLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(MultiTenantAxonServerCommandBusConnector.class);

    private final TenantRouter tenantRouter;
    private final AxonServerConnectionManager connectionManager;
    private final AxonServerConfiguration configuration;
    private final MessageConverter converter;

    private final Map<String, AxonServerCommandBusConnector> tenantConnectors = new ConcurrentHashMap<>();
    private final Map<QualifiedName, Integer> knownSubscriptions = new ConcurrentHashMap<>();

    private final AtomicBoolean started = new AtomicBoolean(false);
    private @Nullable Handler incomingHandler;

    /**
     * Constructs a {@code MultiTenantAxonServerCommandBusConnector}.
     *
     * @param tenantRouter      the router deciding which tenant a dispatched {@link CommandMessage} is routed to
     * @param connectionManager the manager used to obtain the {@link AxonServerConnection} for a given tenant
     * @param configuration     the configuration applied to each per-tenant {@link AxonServerCommandBusConnector}
     * @param converter         the {@link MessageConverter} used by each per-tenant
     *                          {@link AxonServerCommandBusConnector}
     */
    public MultiTenantAxonServerCommandBusConnector(TenantRouter tenantRouter,
                                                    AxonServerConnectionManager connectionManager,
                                                    AxonServerConfiguration configuration,
                                                    MessageConverter converter) {
        this.tenantRouter = Objects.requireNonNull(tenantRouter, "The tenantRouter must not be null.");
        this.connectionManager = Objects.requireNonNull(connectionManager, "The connectionManager must not be null.");
        this.configuration = Objects.requireNonNull(configuration, "The configuration must not be null.");
        this.converter = Objects.requireNonNull(converter, "The converter must not be null.");
    }

    @Override
    public void start() {
        started.set(true);
        tenantConnectors.values().forEach(AxonServerCommandBusConnector::start);
    }

    /**
     * Resolves the connector of the tenant the given {@code command} belongs to and dispatches the command to it.
     *
     * @param command           the command message to dispatch
     * @param processingContext the processing context for the command
     * @return a {@link CompletableFuture} that will complete with the result of the command handling
     */
    @Override
    public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                            @Nullable ProcessingContext processingContext) {
        return resolveConnector(command, processingContext).dispatch(command, processingContext);
    }

    /**
     * Subscribes to a command on each tenant-specific connector with the given {@code commandName} and a
     * {@code loadFactor}.
     * <p>
     * The {@code MultiTenantAxonServerCommandBusConnector} keeps track of all known subscriptions and replays them on
     * tenants dynamically added at runtime.
     *
     * @param commandName the {@link QualifiedName} of the command to subscribe to
     * @param loadFactor  the load factor for the command, which can be used to control the distribution of command
     *                    handling across multiple instances; should be a positive integer
     * @return a {@code CompletableFuture} that completes successfully when this connector subscribed to the given
     * {@code commandName} with the given {@code loadFactor}
     */
    @Override
    public CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor) {
        Integer previousLoadFactor = knownSubscriptions.putIfAbsent(commandName, loadFactor);
        if (previousLoadFactor != null) {
            return FutureUtils.emptyCompletedFuture();
        }
        logger.debug("Subscribing to command [{}] with load factor [{}] across [{}] tenant(s).",
                     commandName, loadFactor, tenantConnectors.size());
        List<CompletableFuture<Void>> acknowledgments = tenantConnectors.values().stream()
                                                                        .map(connector -> connector.subscribe(
                                                                                commandName,
                                                                                loadFactor))
                                                                        .toList();
        return allOrEmpty(acknowledgments);
    }

    /**
     * Unsubscribes from a command with the given {@code commandName} from each tenant-specific connector.
     *
     * @param commandName the {@link QualifiedName} of the command to unsubscribe from
     * @return {@code true} if the unsubscription was successful, {@code false} otherwise
     */
    @Override
    public boolean unsubscribe(QualifiedName commandName) {
        if (knownSubscriptions.remove(commandName) == null) {
            return false;
        }
        logger.debug("Unsubscribing from command [{}] across [{}] tenant(s).", commandName, tenantConnectors.size());
        boolean removed = false;
        for (AxonServerCommandBusConnector connector : tenantConnectors.values()) {
            removed |= connector.unsubscribe(commandName);
        }
        return removed;
    }

    /**
     * Registers a handler that will be called when an incoming command is received. The handler should process the
     * command and call the provided {@code ResultCallback} to indicate success or failure.
     * <p>
     * The handler is registered for each tenant-specific connection to make sure incoming commands from each tenant
     * trigger the handling.
     *
     * @param handler a {@link BiConsumer} that takes a {@link CommandMessage} and a {@link ResultCallback}
     */
    @Override
    public void onIncomingCommand(Handler handler) {
        this.incomingHandler = handler;
        tenantConnectors.values().forEach(connector -> connector.onIncomingCommand(handler));
    }

    @Override
    public CompletableFuture<Void> shutdownDispatching() {
        List<CompletableFuture<Void>> shutdowns = tenantConnectors.values().stream()
                                                                  .map(AxonServerCommandBusConnector::shutdownDispatching)
                                                                  .toList();
        return allOrEmpty(shutdowns);
    }

    @Override
    public CompletableFuture<Void> disconnect() {
        List<CompletableFuture<Void>> disconnects = tenantConnectors.values().stream()
                                                                    .map(AxonServerCommandBusConnector::disconnect)
                                                                    .toList();
        return allOrEmpty(disconnects);
    }

    /**
     * Combines the given per-tenant {@code futures} into a single future, without allocating a no-op
     * {@link CompletableFuture#allOf(CompletableFuture[])} call when there are no tenants to combine.
     *
     * @param futures the per-tenant futures to combine
     * @return a future that completes once every future in {@code futures} completes
     */
    private static CompletableFuture<Void> allOrEmpty(List<CompletableFuture<Void>> futures) {
        return futures.isEmpty()
                ? FutureUtils.emptyCompletedFuture()
                : CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        return addTenant(tenantDescriptor);
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        return addTenant(tenantDescriptor);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tenantRouter", tenantRouter);
        descriptor.describeProperty("subscribedCommands", knownSubscriptions);
        descriptor.describeProperty("tenantConnectors", tenantConnectors);
    }

    /**
     * Resolves the connector of the tenant the given {@code command} belongs to, taking the tenant of the given
     * {@code context} when it carries one, so a command dispatched while handling another message stays with the tenant
     * of that message instead of having to name its tenant again.
     *
     * @param command the command to resolve the connector for
     * @param context the processing context the command is dispatched in, if any
     * @return the connector of the tenant the given {@code command} belongs to
     * @throws TenantNotResolvedException if the command belongs to no known tenant, or to a tenant this connector has
     *                                    no connection for
     */
    private AxonServerCommandBusConnector resolveConnector(CommandMessage command,
                                                           @Nullable ProcessingContext context) {
        // Three steps, in descending authority: the tenant the context carries, the tenant of the message being handled
        // in that context, then the tenant the dispatched command itself names. The router owns the first two.
        TenantDescriptor tenant = tenantRouter.resolveFromContext(context)
                                              .or(() -> tenantRouter.resolveFromMessage(command))
                                              .orElseThrow(tenantNotResolved(
                                                      "No known tenant for command [%s]",
                                                      command.type().qualifiedName()));
        AxonServerCommandBusConnector connector = tenantConnectors.get(tenant.tenantId());
        if (connector == null) {
            logger.warn("No command bus connector found for tenant [{}] while dispatching command [{}].",
                        tenant.tenantId(), command.type().qualifiedName());
            throw TenantNotResolvedException.forTenantId(tenant.tenantId());
        }
        logger.debug("Resolved tenant [{}] for command [{}].", tenant.tenantId(), command.type().qualifiedName());
        return connector;
    }

    private Registration addTenant(TenantDescriptor tenantDescriptor) {
        tenantConnectors.computeIfAbsent(tenantDescriptor.tenantId(), tenantId -> {
            AxonServerCommandBusConnector connector = createConnector(tenantId);
            // Known subscriptions are replayed only while creating a new connector. An already-registered tenant's
            // connector is already in sync, and re-subscribing it would leak an orphaned registration, since
            // AxonServerCommandBusConnector#subscribe(QualifiedName, int) does not cancel a prior registration for the
            // same command name.
            replaySubscriptions(connector, tenantId);
            logger.info("Added command bus connection for tenant [{}]", tenantDescriptor.tenantId());
            return connector;
        });
        return () -> removeTenant(tenantDescriptor);
    }

    private void replaySubscriptions(AxonServerCommandBusConnector connector, String tenantId) {
        for (Map.Entry<QualifiedName, Integer> entry : knownSubscriptions.entrySet()) {
            QualifiedName commandName = entry.getKey();
            // A replay that fails leaves this tenant unable to receive that command, with every other tenant still
            // handling it, so it is reported rather than dropped.
            connector.subscribe(commandName, entry.getValue())
                     .whenComplete((ignored, failure) -> {
                         if (failure != null) {
                             logger.warn("Failed to subscribe tenant [{}] to command [{}].",
                                         tenantId, commandName, failure);
                         }
                     });
        }
        logger.debug("Replayed [{}] known subscription(s) onto tenant [{}].", knownSubscriptions.size(), tenantId);
    }

    private boolean removeTenant(TenantDescriptor tenantDescriptor) {
        AxonServerCommandBusConnector connector = tenantConnectors.remove(tenantDescriptor.tenantId());
        if (connector == null) {
            return false;
        }
        logger.info("Removed command bus connection for tenant [{}]", tenantDescriptor.tenantId());
        connector.disconnect().whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                logger.warn("Failed to disconnect command bus connection for tenant [{}].",
                            tenantDescriptor.tenantId(),
                            throwable);
            }
        });
        return true;
    }

    private AxonServerCommandBusConnector createConnector(String tenantId) {
        AxonServerConnection connection = connectionManager.getConnection(tenantId);
        AxonServerCommandBusConnector connector = new AxonServerCommandBusConnector(connection,
                                                                                    configuration,
                                                                                    converter);
        if (started.get()) {
            connector.start();
        }
        if (incomingHandler != null) {
            connector.onIncomingCommand(incomingHandler);
        }
        return connector;
    }
}
