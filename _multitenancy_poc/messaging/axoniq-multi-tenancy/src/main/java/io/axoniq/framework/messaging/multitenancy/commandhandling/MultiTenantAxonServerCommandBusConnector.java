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

package io.axoniq.framework.messaging.multitenancy.commandhandling;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.command.AbstractAxonServerCommandBusConnector;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.NoSuchTenantException;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Multi-tenant Axon Server {@code CommandBusConnector}.
 * <p>
 * The connector keeps tenant-local connector state per {@link TenantDescriptor}. Each tenant gets its own Axon Server
 * connection, local command handler subscription map, and in-flight command tracker. Command subscription state is
 * replayed to newly registered tenants.
 *
 * @author Jan Galinski
 * @since 5.2.0
 */
public class MultiTenantAxonServerCommandBusConnector extends AbstractAxonServerCommandBusConnector
        implements MultiTenantAwareComponent {

    private final TenantResolver<Message> tenantResolver;
    private final AxonServerConnectionManager connectionManager;

    private final Set<TenantDescriptor> tenantDescriptors = ConcurrentHashMap.newKeySet();
    private final Map<String, TenantState> tenantStates = new ConcurrentHashMap<>();
    private final Map<QualifiedName, Integer> knownSubscriptions = new ConcurrentHashMap<>();

    public MultiTenantAxonServerCommandBusConnector(TenantProvider tenantProvider,
                                                    TenantResolver<Message> tenantResolver,
                                                    AxonServerConnectionManager connectionManager,
                                                    AxonServerConfiguration configuration) {
        this(tenantProvider, tenantResolver, connectionManager, configuration, null);
    }

    public MultiTenantAxonServerCommandBusConnector(TenantProvider tenantProvider,
                                                    TenantResolver<Message> tenantResolver,
                                                    AxonServerConnectionManager connectionManager,
                                                    AxonServerConfiguration configuration,
                                                    @Nullable MessageConverter converter) {
        super(configuration.getClientId(), configuration.getComponentName(), converter);
        this.tenantResolver = Objects.requireNonNull(tenantResolver, "The tenantResolver must not be null.");
        this.connectionManager = Objects.requireNonNull(connectionManager, "The connectionManager must not be null.");
    }

    @Override
    public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                            @Nullable ProcessingContext processingContext) {
        TenantState tenantState = resolveTenant(command);
        return doDispatch(command, tenantState.connection());
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor) {
        Integer previousLoadFactor = knownSubscriptions.putIfAbsent(commandName, loadFactor);
        if (previousLoadFactor != null) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> acknowledgments = tenantStates.values().stream()
                                                                    .map(state -> state.subscribe(commandName,
                                                                                                   loadFactor))
                                                                    .toList();
        return acknowledgments.isEmpty()
                ? CompletableFuture.completedFuture(null)
                : CompletableFuture.allOf(acknowledgments.toArray(CompletableFuture[]::new));
    }

    @Override
    public boolean unsubscribe(QualifiedName commandName) {
        if (knownSubscriptions.remove(commandName) == null) {
            return false;
        }
        boolean removed = false;
        for (TenantState tenantState : tenantStates.values()) {
            removed |= tenantState.unsubscribe(commandName);
        }
        return removed;
    }

    public CompletableFuture<Void> disconnect() {
        List<CompletableFuture<Void>> disconnects = tenantStates.values().stream()
                                                                 .map(TenantState::disconnect)
                                                                 .toList();
        return disconnects.isEmpty()
                ? CompletableFuture.completedFuture(null)
                : CompletableFuture.allOf(disconnects.toArray(CompletableFuture[]::new));
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
        super.describeTo(descriptor);
        descriptor.describeProperty("tenants", tenantDescriptors);
        descriptor.describeProperty("subscribedCommands", knownSubscriptions);
    }

    private TenantState resolveTenant(CommandMessage command) {
        TenantDescriptor tenantDescriptor = tenantResolver.resolveTenant(command, tenantDescriptors);
        TenantState tenantState = tenantStates.get(tenantDescriptor.tenantId());
        if (tenantState == null) {
            throw new NoSuchTenantException(tenantDescriptor.tenantId());
        }
        return tenantState;
    }

    private CompletableFuture<Void> subscribeTenant(QualifiedName commandName,
                                                    int loadFactor,
                                                    AxonServerConnection connection,
                                                    Map<QualifiedName, io.axoniq.axonserver.connector.Registration> subscriptions,
                                                    ConcurrentHashMap<String, CompletableFuture<?>> commandsInProgress) {
        return doSubscribe(commandName,
                           loadFactor,
                           connection,
                           subscriptions,
                           commandsInProgress,
                           Command::getMessageIdentifier);
    }

    private boolean unsubscribeTenant(QualifiedName commandName,
                                      Map<QualifiedName, io.axoniq.axonserver.connector.Registration> subscriptions) {
        return doUnsubscribe(commandName, subscriptions);
    }

    private CompletableFuture<Void> disconnectTenant(AxonServerConnection connection,
                                                     ConcurrentHashMap<String, CompletableFuture<?>> commandsInProgress) {
        return doDisconnect(connection, commandsInProgress.values());
    }

    private Registration addTenant(TenantDescriptor tenantDescriptor) {
        tenantDescriptors.add(tenantDescriptor);
        TenantState tenantState = tenantStates.computeIfAbsent(tenantDescriptor.tenantId(), this::createTenantState);
        replaySubscriptions(tenantState);
        return () -> removeTenant(tenantDescriptor);
    }

    private void replaySubscriptions(TenantState tenantState) {
        for (Map.Entry<QualifiedName, Integer> entry : knownSubscriptions.entrySet()) {
            tenantState.subscribe(entry.getKey(), entry.getValue());
        }
    }

    private boolean removeTenant(TenantDescriptor tenantDescriptor) {
        tenantDescriptors.remove(tenantDescriptor);
        TenantState tenantState = tenantStates.remove(tenantDescriptor.tenantId());
        if (tenantState == null) {
            return false;
        }
        tenantState.disconnect().whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                logger.warn("Failed to disconnect tenant [{}].", tenantDescriptor.tenantId(), throwable);
            }
        });
        return true;
    }

    private TenantState createTenantState(String tenantId) {
        AxonServerConnection connection = connectionManager.getConnection(tenantId);
        return new TenantState(connection);
    }

    private final class TenantState {

        private final AxonServerConnection connection;
        private final Map<QualifiedName, io.axoniq.axonserver.connector.Registration> subscriptions =
                new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, CompletableFuture<?>> commandsInProgress =
                new ConcurrentHashMap<>();

        private TenantState(AxonServerConnection connection) {
            this.connection = connection;
        }

        private AxonServerConnection connection() {
            return connection;
        }

        private CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor) {
            return MultiTenantAxonServerCommandBusConnector.this.subscribeTenant(commandName,
                                                                                loadFactor,
                                                                                connection,
                                                                                subscriptions,
                                                                                commandsInProgress);
        }

        private boolean unsubscribe(QualifiedName commandName) {
            return MultiTenantAxonServerCommandBusConnector.this.unsubscribeTenant(commandName, subscriptions);
        }

        private CompletableFuture<Void> disconnect() {
            return MultiTenantAxonServerCommandBusConnector.this.disconnectTenant(connection, commandsInProgress)
                                                                .thenRun(connection::disconnect);
        }
    }
}
