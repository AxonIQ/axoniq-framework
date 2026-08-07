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

package io.axoniq.framework.messaging.multitenancy.axonserver.queryhandling;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.api.ConnectorLifecycle;
import io.axoniq.framework.axonserver.connector.query.AxonServerQueryBusConnector;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenancyAxoniqAddon;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentLookup;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.axonserver.configuration.AxonServerMultiTenancyConfigurationDefaults;
import io.axoniq.framework.messaging.multitenancy.configuration.TenantComponentProviders;
import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * Multi-tenant Axon Server {@link QueryBusConnector}.
 * <p>
 * The connector composes one {@link AxonServerQueryBusConnector} per {@link TenantDescriptor}, each owning its own Axon
 * Server connection, query handler subscriptions, and in-flight query tracking. Query subscription state and the
 * incoming-query handler are replayed onto newly registered tenants.
 * <p>
 * Internal, because the connector is wired by {@link AxonServerMultiTenancyConfigurationDefaults} and reached through
 * the {@link QueryBusConnector} component, never constructed by an application itself.
 *
 * @author Jan Galinski
 * @author Jakob Hatzl
 * @since 5.3.0
 */
@Internal
public class MultiTenantAxonServerQueryBusConnector
        implements QueryBusConnector, MultiTenantAwareComponent, ConnectorLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(MultiTenantAxonServerQueryBusConnector.class);

    private final TenantRouter tenantRouter;
    private final AxonServerConnectionManager connectionManager;
    private final AxonServerConfiguration configuration;
    private final TenantComponentLookup<MessageConverter> messageConverterLookup;

    private final Set<TenantDescriptor> tenantDescriptors = ConcurrentHashMap.newKeySet();
    private final Map<String, AxonServerQueryBusConnector> tenantConnectors = new ConcurrentHashMap<>();
    private final Set<QualifiedName> knownSubscriptions = ConcurrentHashMap.newKeySet();

    private final AtomicBoolean started = new AtomicBoolean(false);
    private @Nullable Handler incomingHandler;

    /**
     * Constructs a {@code MultiTenantAxonServerQueryBusConnector}.
     *
     * @param tenantRouter      the resolver used to determine the {@link TenantDescriptor} a given {@link QueryMessage}
     *                          belongs to
     * @param connectionManager the manager used to obtain the {@link AxonServerConnection} for a given tenant
     * @param configuration     the configuration applied to each per-tenant {@link AxonServerQueryBusConnector}
     * @param converter         the {@link MessageConverter} used by each per-tenant
     *                          {@link AxonServerQueryBusConnector}
     */
    public MultiTenantAxonServerQueryBusConnector(TenantRouter tenantRouter,
                                                  AxonServerConnectionManager connectionManager,
                                                  AxonServerConfiguration configuration,
                                                  MessageConverter converter) {
        this(tenantRouter,
             connectionManager,
             configuration,
             TenantComponentProviders.defaultTenantComponentLookup(converter)
        );
    }

    /**
     * Constructs a connector that obtains the message converter for every tenant while constructing that tenant's
     * connector.
     *
     * @param tenantRouter           the router deciding which tenant a dispatched query is routed to
     * @param connectionManager      the manager used to obtain the connection for a given tenant
     * @param configuration          the configuration applied to each per-tenant connector
     * @param messageConverterLookup the lookup providing the message converter for each tenant
     */
    public MultiTenantAxonServerQueryBusConnector(TenantRouter tenantRouter,
                                                  AxonServerConnectionManager connectionManager,
                                                  AxonServerConfiguration configuration,
                                                  TenantComponentLookup<MessageConverter> messageConverterLookup) {
        this.tenantRouter = requireNonNull(tenantRouter, "The tenantRouter must not be null.");
        this.connectionManager = requireNonNull(connectionManager, "The connectionManager must not be null.");
        this.configuration = requireNonNull(configuration, "The configuration must not be null.");
        this.messageConverterLookup = requireNonNull(messageConverterLookup,
                                                     "The messageConverterLookup must not be null.");

        EntitlementManager.INSTANCE.registerAddon(MultiTenancyAxoniqAddon.class);
    }

    @Override
    public void start() {
        started.set(true);
        tenantConnectors.values().forEach(AxonServerQueryBusConnector::start);
    }

    /**
     * Resolves the {@link TenantResolver connector for the current tenant} and subsequently dispatches the given
     * {@code query} to it.
     *
     * @param query   the query message to dispatch
     * @param context the processing context for the query
     * @return a {@link MessageStream} of the responses for the query
     */
    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query, @Nullable ProcessingContext context) {
        return resolveConnector(query, context).query(query, context);
    }

    /**
     * Resolves the {@link TenantResolver connector for the current tenant} and subsequently dispatches the given
     * subscription {@code query} to it.
     *
     * @param query            the subscription query message to dispatch
     * @param context          the processing context for the query
     * @param updateBufferSize the size of the buffer used to store updates for the subscription query
     * @return a {@link MessageStream} of the responses for the query
     */
    @Override
    public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                 @Nullable ProcessingContext context,
                                                                 int updateBufferSize) {
        return resolveConnector(query, context).subscriptionQuery(query, context, updateBufferSize);
    }

    /**
     * Subscribes to a query on each tenant-specific connector with the given {@code queryName}.
     * <p>
     * The {@code MultiTenantAxonServerQueryBusConnector} keeps track of all known subscriptions and replays them on
     * tenants dynamically added at runtime.
     *
     * @param queryName the {@link QualifiedName} of the query to subscribe to
     * @return a {@code CompletableFuture} that completes successfully when this connector subscribed to the given
     * {@code queryName}
     */
    @Override
    public CompletableFuture<Void> subscribe(QualifiedName queryName) {
        if (!knownSubscriptions.add(queryName)) {
            return FutureUtils.emptyCompletedFuture();
        }
        logger.debug("Subscribing to query [{}] across [{}] tenant(s).", queryName, tenantConnectors.size());
        List<CompletableFuture<Void>> acknowledgments = tenantConnectors.values().stream()
                                                                        .map(connector -> connector.subscribe(
                                                                                queryName))
                                                                        .toList();
        return FutureUtils.allOrEmpty(acknowledgments);
    }

    /**
     * Unsubscribes from a query with the given {@code queryName} from each tenant-specific connector.
     *
     * @param queryName the {@link QualifiedName} of the query to unsubscribe from
     * @return {@code true} if the unsubscription was successful, {@code false} otherwise
     */
    @Override
    public boolean unsubscribe(QualifiedName queryName) {
        if (!knownSubscriptions.remove(queryName)) {
            return false;
        }
        logger.debug("Unsubscribing from query [{}] across [{}] tenant(s).", queryName, tenantConnectors.size());
        boolean removed = false;
        for (AxonServerQueryBusConnector connector : tenantConnectors.values()) {
            removed |= connector.unsubscribe(queryName);
        }
        return removed;
    }

    /**
     * Registers a handler that will be called when an incoming query is received.
     * <p>
     * The handler is registered for each tenant-specific connection to make sure incoming queries from each tenant
     * trigger the handling.
     *
     * @param handler the {@link Handler} responsible for managing incoming queries
     */
    @Override
    public void onIncomingQuery(Handler handler) {
        this.incomingHandler = handler;
        tenantConnectors.values().forEach(connector -> connector.onIncomingQuery(handler));
    }

    @Override
    public CompletableFuture<Void> shutdownDispatching() {
        List<CompletableFuture<Void>> shutdowns = tenantConnectors.values().stream()
                                                                  .map(AxonServerQueryBusConnector::shutdownDispatching)
                                                                  .toList();
        return FutureUtils.allOrEmpty(shutdowns);
    }

    @Override
    public CompletableFuture<Void> disconnect() {
        List<CompletableFuture<Void>> disconnects = tenantConnectors.values().stream()
                                                                    .map(AxonServerQueryBusConnector::disconnect)
                                                                    .toList();
        return FutureUtils.allOrEmpty(disconnects);
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
        descriptor.describeProperty("tenants", tenantDescriptors);
        descriptor.describeProperty("subscribedQueries", knownSubscriptions);
        descriptor.describeProperty("tenantConnectors", tenantConnectors);
    }

    /**
     * Resolves the connector of the tenant the given {@code query} belongs to, taking the tenant of the given
     * {@code context} when it carries one, so a query dispatched while handling another message stays with the tenant
     * of that message instead of having to name its tenant again.
     *
     * @param query   the query to resolve the connector for
     * @param context the processing context the query is dispatched in, if any
     * @return the connector of the tenant the given {@code query} belongs to
     * @throws TenantNotResolvedException if the query belongs to no known tenant, or to a tenant this connector has no
     *                                    connection for
     */
    private AxonServerQueryBusConnector resolveConnector(QueryMessage query,
                                                         @Nullable ProcessingContext context) {
        // Three steps, in descending authority: the tenant the context carries, the tenant of the message being handled
        // in that context, then the tenant the dispatched query itself names. The router owns the first two.
        TenantDescriptor tenantDescriptor = tenantRouter.resolveFromContext(context)
                                                        .or(() -> tenantRouter.resolveFromMessage(query))
                                                        .orElseThrow(TenantNotResolvedException.tenantNotResolved(
                                                                "No known tenant for query [%s]",
                                                                query.type().qualifiedName()));
        AxonServerQueryBusConnector connector = tenantConnectors.get(tenantDescriptor.tenantId());
        if (connector == null) {
            logger.warn("No query bus connector found for tenant [{}] while dispatching query [{}].",
                        tenantDescriptor.tenantId(), query.type().qualifiedName());
            throw TenantNotResolvedException.forTenantId(tenantDescriptor.tenantId());
        }
        logger.debug("Resolved tenant [{}] for query [{}].", tenantDescriptor.tenantId(), query.type().qualifiedName());
        return connector;
    }

    private Registration addTenant(TenantDescriptor tenantDescriptor) {
        tenantDescriptors.add(tenantDescriptor);
        tenantConnectors.computeIfAbsent(tenantDescriptor.tenantId(), tenantId -> {
            AxonServerQueryBusConnector connector = createConnector(tenantDescriptor);
            // Known subscriptions are replayed only while creating a new connector. An already-registered tenant's
            // connector is already in sync, and re-subscribing it would be a needless no-op at best; at worst it
            // risks the same orphaned-registration pitfall the command bus connector guards against.
            replaySubscriptions(connector, tenantId);
            logger.info("Added query bus connection for tenant [{}]", tenantDescriptor.tenantId());
            return connector;
        });
        return () -> removeTenant(tenantDescriptor);
    }

    private void replaySubscriptions(AxonServerQueryBusConnector connector, String tenantId) {
        for (QualifiedName subscription : knownSubscriptions) {
            connector.subscribe(subscription);
        }
        logger.debug("Replayed [{}] known subscription(s) onto tenant [{}].", knownSubscriptions.size(), tenantId);
    }

    private boolean removeTenant(TenantDescriptor tenantDescriptor) {
        tenantDescriptors.remove(tenantDescriptor);
        AxonServerQueryBusConnector connector = tenantConnectors.remove(tenantDescriptor.tenantId());
        if (connector == null) {
            return false;
        }
        logger.info("Removed query bus connection for tenant [{}]", tenantDescriptor.tenantId());
        connector.disconnect().whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                logger.warn("Failed to disconnect query bus connection for tenant [{}].",
                            tenantDescriptor.tenantId(),
                            throwable);
            }
        });
        return true;
    }

    private AxonServerQueryBusConnector createConnector(TenantDescriptor tenant) {
        AxonServerConnection connection = connectionManager.getConnection(tenant.tenantId());
        AxonServerQueryBusConnector connector = new AxonServerQueryBusConnector(
                connection,
                configuration,
                messageConverterLookup.componentFor(tenant)
        );

        if (started.get()) {
            connector.start();
        }
        if (incomingHandler != null) {
            connector.onIncomingQuery(incomingHandler);
        }
        return connector;
    }
}
