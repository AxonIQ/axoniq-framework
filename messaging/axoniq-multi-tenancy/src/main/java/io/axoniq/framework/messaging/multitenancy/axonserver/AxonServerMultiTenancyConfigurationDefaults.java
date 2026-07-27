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

package io.axoniq.framework.messaging.multitenancy.axonserver;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantSnapshotStoreFactory;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationDefaults;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentLifecycleHandler;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.MultiTenancyEnabled.isEnabled;

/**
 * {@link ConfigurationEnhancer} registering the default Axon Server-backed multi-tenancy components:
 * <ul>
 *     <li>the {@link TenantProvider} which is responsible for managing tenants and lifecycle, backed by {@link AxonServerTenantProvider}</li>
 *     <li>the {@link MultiTenantAxonServerCommandBusConnector} which fans out commands to a connector per tenant</li>
 *     <li>the {@link MultiTenantEventStorageEngine}, registered as both the {@link EventStorageEngine} and the
 *     {@link SnapshotStore}, routing writes, sourcing, and snapshots to each tenant's own components</li>
 *     <li>the {@link TenantEventStorageEngineFactory} and {@link TenantSnapshotStoreFactory} building those per-tenant
 *     components against the tenant's Axon Server context</li>
 * </ul>
 *
 * @author Jan Galinski
 * @author Laura Devriendt
 * @author Jakob Hatzl
 * @since 5.3.0
 */
@Internal
@RegistrationScope(scope = RegistrationScope.Scope.CURRENT)
public class AxonServerMultiTenancyConfigurationDefaults implements ConfigurationEnhancer {

    /**
     * The order of {@code this} enhancer compared to others.
     * <p>
     * Runs early, so the Axon Server-backed multi-tenancy defaults registered here are in place before other enhancers
     * and user registrations that build on them, most notably before Axon Server's own
     * {@code AxonServerConfigurationEnhancer} registers its non-multi-tenant defaults for the same component types.
     */
    public static final int ENHANCER_ORDER = Integer.MIN_VALUE + 7;

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        // TODO: see #258 - find a way that is not user facing but only needed for our mixed-scope itests.
        if (!isEnabled(componentRegistry)) {
            return;
        }

        // Register the Axon Server TenantProvider, which is the default implementation for multi-tenancy in Axon Server.
        componentRegistry.registerIfNotPresent(axonServerTenantProvider(), SearchScope.ALL);

        // Register the MultiTenantAxonServerCommandBusConnector
        componentRegistry.registerIfNotPresent(multiTenantCommandBusConnector(), SearchScope.ALL);

        // Register the multi-tenant EventStorageEngine, routing writes, sourcing, and snapshots to each tenant's own
        // engine and snapshot store.
        registerMultiTenantEventStorageEngine(componentRegistry);
    }

    /**
     * Provides a {@link ComponentDefinition} for the {@link TenantProvider} that is backed by Axon Server. Uses the
     * {@link AxonServerConnectionManager} to manage connections and the {@link TenantConnectPredicate} to determine
     * which tenants to connect to, defaults to {@link AxonServerTenantConnectPredicate}.
     *
     * @return a {@link ComponentDefinition} for the {@link TenantProvider} that is backed by Axon Server
     */
    public static ComponentDefinition<TenantProvider> axonServerTenantProvider() {
        return ComponentDefinition
                .ofType(TenantProvider.class)
                .withBuilder(config -> new AxonServerTenantProvider(
                                     config.getComponent(AxonServerConnectionManager.class),
                                     config.getComponent(TenantConnectPredicate.class, AxonServerTenantConnectPredicate::new)
                             )
                )
                .onStart(MultiTenancyConfigurationDefaults.TENANT_PROVIDER_PHASE,
                         (ComponentLifecycleHandler<TenantProvider>) (config, provider) ->
                                 ((AxonServerTenantProvider) provider).start())
                .onShutdown(MultiTenancyConfigurationDefaults.TENANT_PROVIDER_PHASE,
                            (ComponentLifecycleHandler<TenantProvider>) (config, provider) ->
                                    ((AxonServerTenantProvider) provider).shutdown());
    }

    /**
     * Builds the {@link MultiTenantAxonServerCommandBusConnector}, subscribing it to the {@link TenantProvider} at
     * startup so it follows the tenant lifecycle: known tenants are replayed on subscription and tenants added or
     * removed at runtime reach it. Shutdown handlers run in descending phase order, so at shutdown the connector is
     * disconnected first ({@link Phase#INBOUND_COMMAND_CONNECTOR}), then its dispatching is shut down
     * ({@link Phase#OUTBOUND_COMMAND_CONNECTORS}), before the retained tenant-provider subscription is cancelled last.
     * <p>
     * The tenant-provider subscription is retained on the {@code connector} instance received by the lifecycle handlers
     * directly, rather than re-resolved through {@link Configuration#getComponent(Class)} for
     * {@link CommandBusConnector}: other {@link ConfigurationEnhancer ConfigurationEnhancers} may decorate that type
     * (for example to convert payloads), and looking it up again would subscribe the decorator instead of the
     * underlying, multi-tenant aware connector built here.
     *
     * @return a {@link ComponentDefinition} for the {@link MultiTenantAxonServerCommandBusConnector}
     */
    private static ComponentDefinition<CommandBusConnector> multiTenantCommandBusConnector() {
        AtomicReference<@Nullable Registration> tenantSubscription = new AtomicReference<>();
        return ComponentDefinition.ofType(CommandBusConnector.class)
                                  .withBuilder(config -> new MultiTenantAxonServerCommandBusConnector(
                                          config.getComponent(TenantResolver.class),
                                          config.getComponent(AxonServerConnectionManager.class),
                                          config.getComponent(AxonServerConfiguration.class),
                                          config.getComponent(MessageConverter.class)))
                                  .onStart(MultiTenancyConfigurationDefaults.TENANT_COMPONENT_SUBSCRIBER_PHASE,
                                           (config, connector) -> {
                                               tenantSubscription.set(config.getComponent(TenantProvider.class)
                                                                            .subscribe((MultiTenantAxonServerCommandBusConnector) connector));
                                               return FutureUtils.emptyCompletedFuture();
                                           })
                                  .onStart(Phase.INBOUND_COMMAND_CONNECTOR,
                                           connector -> ((MultiTenantAxonServerCommandBusConnector) connector).start())
                                  .onShutdown(Phase.OUTBOUND_COMMAND_CONNECTORS,
                                              (ComponentLifecycleHandler<CommandBusConnector>) (config, connector) ->
                                                      ((MultiTenantAxonServerCommandBusConnector) connector).shutdownDispatching())
                                  .onShutdown(Phase.INBOUND_COMMAND_CONNECTOR,
                                              (ComponentLifecycleHandler<CommandBusConnector>) (config, connector) ->
                                                      ((MultiTenantAxonServerCommandBusConnector) connector).disconnect())
                                  .onShutdown(MultiTenancyConfigurationDefaults.TENANT_COMPONENT_SUBSCRIBER_PHASE,
                                              (config, connector) -> {
                                                  Registration registration = tenantSubscription.getAndSet(null);
                                                  if (registration != null) {
                                                      registration.cancel();
                                                  }
                                                  return FutureUtils.emptyCompletedFuture();
                                              });
    }

    /**
     * Registers the {@link MultiTenantEventStorageEngine} as both the {@link EventStorageEngine} and the
     * {@link SnapshotStore}, backed by a {@link TenantEventStorageEngineFactory} and a
     * {@link TenantSnapshotStoreFactory} that are subscribed to the {@link TenantProvider} so per-tenant engines and
     * snapshot stores are evicted when a tenant is removed. Registered before the Axon Server enhancer, whose
     * {@code registerIfNotPresent} for both types then backs off.
     * <p>
     * Both types resolve to the <em>same</em> instance on purpose. The event sourcing defaults leave an engine
     * undecorated only when it is the registered {@link SnapshotStore} itself, compared by identity. Any other
     * {@link SnapshotStore} would have them complement the routing engine above the tenant fan-out, resolving snapshots
     * before a tenant is known, which is why a {@link SnapshotStore} registered elsewhere is rejected here.
     *
     * @param componentRegistry the registry to register the routing engine and its factories with
     */
    static void registerMultiTenantEventStorageEngine(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(
                subscribedFactory(TenantSnapshotStoreFactory.class,
                                  AxonServerTenantSnapshotStoreFactory::new),
                SearchScope.ALL);
        componentRegistry.registerIfNotPresent(
                subscribedFactory(TenantEventStorageEngineFactory.class,
                                  AxonServerTenantEventStorageEngineFactory::new),
                SearchScope.ALL);

        // The EventStorageEngine and the SnapshotStore have to resolve to the same instance, because the event sourcing
        // defaults leave an engine undecorated only when it is the registered SnapshotStore itself, compared by
        // identity. The routing engine is therefore built as the SnapshotStore, and the EventStorageEngine resolves
        // that one component. Only this direction works: the engine's own decorator asks for the SnapshotStore while
        // the engine is being resolved, so aliasing the SnapshotStore to the engine re-enters that resolution.
        //
        // Both stay lazy, built on first use rather than at startup, since resolving them pulls in the per-tenant
        // factories. Resolving the engine therefore also verifies the two are one instance.
        componentRegistry.registerIfNotPresent(
                ComponentDefinition.ofType(SnapshotStore.class)
                                   .withBuilder(AxonServerMultiTenancyConfigurationDefaults::routingEngine),
                SearchScope.ALL);
        componentRegistry.registerIfNotPresent(
                ComponentDefinition.ofType(EventStorageEngine.class)
                                   .withBuilder(AxonServerMultiTenancyConfigurationDefaults
                                                        ::routingEngineRegisteredAsSnapshotStore),
                SearchScope.ALL);
    }

    private static MultiTenantEventStorageEngine routingEngine(Configuration config) {
        return new MultiTenantEventStorageEngine(config.getComponent(TenantEventStorageEngineFactory.class),
                                                config.getComponent(TenantSnapshotStoreFactory.class),
                                                config.getComponent(TenantResolver.class),
                                                config.getComponent(TenantProvider.class));
    }

    /**
     * Resolves the routing engine through the {@link SnapshotStore} it is registered as, so the
     * {@link EventStorageEngine} is that same instance rather than a second one.
     *
     * @param config the configuration to resolve the {@link SnapshotStore} from
     * @return the routing engine registered as the {@link SnapshotStore}
     * @throws AxonConfigurationException if the registered {@link SnapshotStore} is not the routing engine
     */
    private static MultiTenantEventStorageEngine routingEngineRegisteredAsSnapshotStore(Configuration config) {
        SnapshotStore snapshotStore = config.getComponent(SnapshotStore.class);
        if (snapshotStore instanceof MultiTenantEventStorageEngine routingEngine) {
            return routingEngine;
        }
        throw new AxonConfigurationException("""
                A multi-tenant application requires the SnapshotStore to be the multi-tenant routing engine, so \
                snapshots are resolved per tenant rather than from one store shared by every tenant, but it resolved \
                to [%s]. Register a TenantSnapshotStoreFactory to control how each tenant's snapshot store is built, \
                instead of replacing the SnapshotStore itself.""".formatted(snapshotStore.getClass().getName()));
    }

    /**
     * Builds a {@link ComponentDefinition} for a per-tenant component factory. When the built factory is a
     * {@link MultiTenantAwareComponent}, it is subscribed to the {@link TenantProvider} at startup and unsubscribed at
     * shutdown, so tenant additions and removals reach the factory's cache.
     *
     * @param factoryType the component type of the factory
     * @param builder     the builder constructing the factory from the {@link Configuration}
     * @param <F>         the factory type
     * @return a {@link ComponentDefinition} for the subscribed factory
     */
    private static <F> ComponentDefinition<F> subscribedFactory(Class<F> factoryType,
                                                                Function<Configuration, F> builder) {
        AtomicReference<@Nullable Registration> subscription = new AtomicReference<>();
        return ComponentDefinition.ofType(factoryType)
                                  .withBuilder(builder::apply)
                                  .onStart(MultiTenancyConfigurationDefaults.TENANT_COMPONENT_SUBSCRIBER_PHASE,
                                           (config, factory) -> {
                                               if (factory instanceof MultiTenantAwareComponent aware) {
                                                   subscription.set(config.getComponent(TenantProvider.class)
                                                                           .subscribe(aware));
                                               }
                                               return FutureUtils.emptyCompletedFuture();
                                           })
                                  .onShutdown(MultiTenancyConfigurationDefaults.TENANT_COMPONENT_SUBSCRIBER_PHASE,
                                              (config, factory) -> {
                                                  Registration registration = subscription.getAndSet(null);
                                                  if (registration != null) {
                                                      registration.cancel();
                                                  }
                                                  return FutureUtils.emptyCompletedFuture();
                                              });
    }
}
