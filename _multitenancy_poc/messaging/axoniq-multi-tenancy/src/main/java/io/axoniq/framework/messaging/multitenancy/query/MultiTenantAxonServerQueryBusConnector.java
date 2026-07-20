/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
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

package io.axoniq.framework.messaging.multitenancy.query;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.query.AbstractAxonServerQueryBusConnector;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.NoSuchTenantException;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Multi-tenant Axon Server {@code QueryBusConnector}.
 * <p>
 * The connector keeps tenant-local connector state per {@link TenantDescriptor}. Each tenant gets its own Axon Server
 * connection, local query handler subscription map, and in-flight query tracker. Query subscription state is replayed
 * to newly registered tenants.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
public class MultiTenantAxonServerQueryBusConnector extends AbstractAxonServerQueryBusConnector
        implements MultiTenantAwareComponent {

    private final TenantResolver<org.axonframework.messaging.core.Message> tenantResolver;
    private final AxonServerConnectionManager connectionManager;

    private final Set<TenantDescriptor> tenantDescriptors = ConcurrentHashMap.newKeySet();
    private final Map<String, TenantState> tenantStates = new ConcurrentHashMap<>();
    private final Set<QualifiedName> knownSubscriptions = ConcurrentHashMap.newKeySet();

    /**
     * Creates a new MultiTenantAxonServerQueryBusConnector with the given {@code tenantProvider},
     *
     * @param tenantProvider    tenant provider to subscribe to for tenant changes and retrieve the current set of
     *                          tenants from
     * @param tenantResolver    tenant resolver to determine the target tenant from a query message
     * @param connectionManager connection manager to obtain connections for each tenant
     * @param configuration     configuration to use for the connector
     */
    public MultiTenantAxonServerQueryBusConnector(TenantProvider tenantProvider,
                                                  TenantResolver<org.axonframework.messaging.core.Message> tenantResolver,
                                                  AxonServerConnectionManager connectionManager,
                                                  AxonServerConfiguration configuration) {
        this(tenantProvider, tenantResolver, connectionManager, configuration, null);
    }

    /**
     * Creates a new MultiTenantAxonServerQueryBusConnector with the given {@code tenantProvider},
     * {@code tenantResolver}, {@code connectionManager} and {@code configuration}.
     *
     * @param tenantProvider    tenant provider to subscribe to for tenant changes and retrieve the current set of
     *                          tenants from
     * @param tenantResolver    tenant resolver to determine the target tenant from a query message
     * @param connectionManager connection manager to obtain connections for each tenant
     * @param configuration     configuration to use for the connector
     * @param converter         message converter to use for converting messages
     */
    public MultiTenantAxonServerQueryBusConnector(TenantProvider tenantProvider,
                                                  TenantResolver<org.axonframework.messaging.core.Message> tenantResolver,
                                                  AxonServerConnectionManager connectionManager,
                                                  AxonServerConfiguration configuration,
                                                  @Nullable MessageConverter converter) {
        super(configuration.getClientId(), configuration.getComponentName(), converter);
        this.tenantResolver = Objects.requireNonNull(tenantResolver, "The tenantResolver must not be null.");
        this.connectionManager = Objects.requireNonNull(connectionManager, "The connectionManager must not be null.");
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName name) {
        if (!knownSubscriptions.add(name)) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> acknowledgments = tenantStates.values().stream()
                                                                    .map(state -> state.subscribe(name))
                                                                    .toList();
        return acknowledgments.isEmpty()
                ? CompletableFuture.completedFuture(null)
                : CompletableFuture.allOf(acknowledgments.toArray(CompletableFuture[]::new));
    }

    @Override
    public boolean unsubscribe(QualifiedName name) {
        if (!knownSubscriptions.remove(name)) {
            return false;
        }
        boolean removed = false;
        for (TenantState tenantState : tenantStates.values()) {
            removed |= tenantState.unsubscribe(name);
        }
        return removed;
    }

    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query,
                                                     @Nullable ProcessingContext context) {
        TenantState tenantState = resolveTenant(query);
        return doQuery(query, tenantState.connection());
    }

    @Override
    public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                 @Nullable ProcessingContext context,
                                                                 int updateBufferSize) {
        TenantState tenantState = resolveTenant(query);
        return doSubscriptionQuery(query, tenantState.connection(), updateBufferSize);
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
        descriptor.describeProperty("subscribedQueries", knownSubscriptions);
    }

    private TenantState resolveTenant(QueryMessage query) {
        TenantDescriptor tenantDescriptor = tenantResolver.resolveTenant(query, tenantDescriptors);
        TenantState tenantState = tenantStates.get(tenantDescriptor.tenantId());
        if (tenantState == null) {
            throw new NoSuchTenantException(tenantDescriptor.tenantId());
        }
        return tenantState;
    }

    private Registration addTenant(TenantDescriptor tenantDescriptor) {
        tenantDescriptors.add(tenantDescriptor);
        TenantState tenantState = tenantStates.computeIfAbsent(tenantDescriptor.tenantId(), this::createTenantState);
        replaySubscriptions(tenantState);
        return () -> removeTenant(tenantDescriptor);
    }

    private void replaySubscriptions(TenantState tenantState) {
        for (QualifiedName subscription : knownSubscriptions) {
            tenantState.subscribe(subscription);
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
        private final Map<String, Runnable> queriesInProgress = new ConcurrentHashMap<>();
        private final LocalSegmentAdapter localSegmentAdapter = newLocalSegmentAdapter(queriesInProgress);

        private TenantState(AxonServerConnection connection) {
            this.connection = connection;
        }

        private AxonServerConnection connection() {
            return connection;
        }

        private CompletableFuture<Void> subscribe(QualifiedName name) {
            return doSubscribe(connection, name, localSegmentAdapter, subscriptions);
        }

        private boolean unsubscribe(QualifiedName name) {
            return doUnsubscribe(name, subscriptions);
        }

        private CompletableFuture<Void> disconnect() {
            return doDisconnect(connection, localSegmentAdapter)
                    .thenRun(connection::disconnect);
        }
    }
}
